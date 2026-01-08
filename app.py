import streamlit as st
import pandas as pd
import pdfplumber
import re
from datetime import datetime
import time
import base64
import hashlib
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from gspread.exceptions import APIError

# ==========================================
# 1. CẤU HÌNH TRANG
# ==========================================
st.set_page_config(page_title="Quản Lý Hóa Đơn Pro", page_icon="📑", layout="wide")

# ==========================================
# 2. KẾT NỐI (CÓ CACHE & AN TOÀN)
# ==========================================
@st.cache_resource
def get_creds():
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        scope = [
            'https://www.googleapis.com/auth/spreadsheets', 
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return creds
    except Exception as e:
        # Không báo lỗi đỏ, chỉ log ra terminal để tránh vỡ giao diện
        print(f"Lỗi credentials: {e}")
        return None

def get_gspread_client():
    creds = get_creds()
    if creds: return gspread.authorize(creds)
    return None

def get_drive_service():
    creds = get_creds()
    if creds: return build('drive', 'v3', credentials=creds)
    return None

def get_db():
    client = get_gspread_client()
    if client:
        try: return client.open_by_url(st.secrets["sheets"]["url"])
        except: return None
    return None

# --- HÀM AN TOÀN CHỐNG QUOTA LIMIT & NULL ---
def safe_get_worksheet(sh, title):
    if sh is None: return None # <--- FIX QUAN TRỌNG: Nếu không có kết nối thì trả về None ngay
    max_retries = 3
    for i in range(max_retries):
        try: return sh.worksheet(title)
        except APIError as e:
            if e.response.status_code == 429: time.sleep((2 ** i) + 1)
            else: return None # Nếu lỗi khác (vd không tìm thấy sheet) thì bỏ qua
        except: return None
    return None

def safe_get_all_records(ws):
    if ws is None: return [] # <--- FIX: Nếu worksheet không tồn tại thì trả về list rỗng
    max_retries = 3
    for i in range(max_retries):
        try: return ws.get_all_records()
        except APIError as e:
            if e.response.status_code == 429: time.sleep((2 ** i) + 1)
            else: return []
        except: return []
    return []

# --- KHỞI TẠO DB ---
def init_db():
    sh = get_db()
    if sh is None: return

    tables = {
        'users': ['id', 'username', 'password', 'role', 'status'],
        'invoices': ['id', 'type', 'group_name', 'date', 'invoice_number', 'invoice_symbol', 
                     'seller_name', 'seller_tax', 'buyer_name', 'pre_tax_amount', 'tax_amount', 
                     'total_amount', 'file_name', 'status', 'edit_count', 'created_at', 'memo', 'drive_url'],
        'projects': ['id', 'project_name', 'created_at'],
        'project_links': ['id', 'project_id', 'invoice_id'],
        'company_info': ['id', 'name', 'address', 'phone', 'logo_base64', 'bg_color', 'text_color', 'box_color']
    }

    try:
        current_titles = [w.title for w in sh.worksheets()]
        for table_name, headers in tables.items():
            if table_name not in current_titles:
                ws = sh.add_worksheet(title=table_name, rows=100, cols=20)
                ws.append_row(headers)
                if table_name == 'users':
                    admin_pw = hashlib.sha256("admin123".encode()).hexdigest()
                    ws.append_row([1, 'Admin', admin_pw, 'admin', 'approved'])
                if table_name == 'company_info':
                    ws.append_row([1, 'Tên Công Ty Của Bé', 'Địa chỉ', 'SĐT', '', '', '', ''])
            else:
                if table_name == 'invoices':
                    ws = safe_get_worksheet(sh, 'invoices')
                    if ws:
                        current_headers = ws.row_values(1)
                        if 'drive_url' not in current_headers:
                            ws.update_cell(1, len(current_headers) + 1, 'drive_url')
    except: pass

if 'db_initialized' not in st.session_state:
    init_db()
    st.session_state.db_initialized = True

# --- CÁC HÀM HỖ TRỢ KHÁC ---
def get_next_id(worksheet):
    if worksheet is None: return 1
    col_values = worksheet.col_values(1)
    if len(col_values) <= 1: return 1 
    try:
        ids = [int(x) for x in col_values[1:] if str(x).isdigit()]
        return max(ids) + 1 if ids else 1
    except: return 1

def hash_pass(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- FIX: Hàm lấy data công ty an toàn tuyệt đối ---
@st.cache_data(ttl=600) 
def get_company_data():
    default_data = {'name': 'Tên Công Ty', 'address': '...', 'phone': '...', 'logo': None}
    sh = get_db()
    if not sh: return pd.Series(default_data)
    
    try:
        ws = safe_get_worksheet(sh, 'company_info')
        data = safe_get_all_records(ws)
        if data:
            row = data[0]
            if row.get('logo_base64'):
                row['logo'] = base64.b64decode(row['logo_base64'])
            else:
                row['logo'] = None
            return pd.Series(row)
    except: pass
    return pd.Series(default_data)

def update_company_info(name, address, phone, logo_bytes=None):
    sh = get_db()
    if not sh: return
    ws = safe_get_worksheet(sh, 'company_info')
    if not ws: return
    ws.update_cell(2, 2, name)
    ws.update_cell(2, 3, address)
    ws.update_cell(2, 4, phone)
    if logo_bytes:
        b64_str = base64.b64encode(logo_bytes).decode('utf-8')
        ws.update_cell(2, 5, b64_str)
    get_company_data.clear()

def upload_to_drive(file_obj, file_name):
    try:
        service = get_drive_service()
        if not service: return None, "Mất kết nối API Drive"
        
        folder_id = None
        try: folder_id = st.secrets["drive"]["folder_id"]
        except: pass

        file_metadata = {'name': file_name}
        if folder_id: file_metadata['parents'] = [folder_id]
        
        file_content = file_obj.getvalue()
        buffer = BytesIO(file_content)
        media = MediaIoBaseUpload(buffer, mimetype='application/pdf', resumable=True)
        
        file = service.files().create(
            body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True 
        ).execute()
        return file.get('webViewLink'), None

    except Exception as e:
        err_msg = str(e)
        if "Service Accounts do not have storage quota" in err_msg or "storageQuotaExceeded" in err_msg:
            return None, "QUOTA_ERROR"
        return None, err_msg

# ==========================================
# 3. CSS & GIAO DIỆN
# ==========================================
# Khởi tạo comp AN TOÀN ở cấp cao nhất
comp = get_company_data()

st.markdown("""
<style>
    .stApp { background-color: var(--background-color); color: var(--text-color); font-family: 'Segoe UI', sans-serif; }
    .money-box { 
        background: linear-gradient(135deg, #1e7e34 0%, #28a745 100%) !important;
        color: #ffffff !important; padding: 20px; border-radius: 12px; 
        box-shadow: 0 4px 15px rgba(40, 167, 69, 0.4); font-size: 1.2em; font-weight: bold; text-align: center; border: none;
    }
    .report-card, .login-container { 
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 12px; padding: 20px; margin-bottom: 15px; 
        color: var(--text-color) !important; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: transform 0.2s; 
    }
    .report-card:hover { transform: translateY(-3px); border-color: #28a745; box-shadow: 0 6px 12px rgba(40, 167, 69, 0.2); }
    .stButton button { border-radius: 8px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; transition: all 0.3s; }
    .company-header { display: flex; align-items: center; justify-content: center; gap: 25px; margin-bottom: 30px; border-bottom: 1px solid rgba(128, 128, 128, 0.2); padding-bottom: 20px; background: transparent; padding: 20px; }
    .header-logo { border-radius: 10px; object-fit: contain; }
    .login-container { max-width: 500px; margin: 0 auto; padding: 40px; text-align: center; }
    .time-badge { background-color: var(--secondary-background-color); color: #28a745; padding: 4px 12px; border-radius: 20px; font-size: 0.85em; font-weight: bold; border: 1px solid #28a745; }
    
    h1, h2, h3, h4, h5, p, span, div, label { color: var(--text-color) !important; }
    .stAlert p, .stAlert div, .stAlert h4, .stAlert span { color: inherit !important; }
    .money-box b, .money-box div { color: #ffffff !important; }
    
    .stTextInput input, .stNumberInput input { color: var(--text-color) !important; background-color: var(--secondary-background-color) !important; border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 8px; }
    .stTextInput input:focus, .stNumberInput input:focus { border-color: #28a745 !important; box-shadow: 0 0 0 1px #28a745; }
    
    input:disabled, div[data-testid="stNumberInput"] input[disabled], div[data-testid="stTextInput"] input[disabled] {
        opacity: 1 !important; color: var(--text-color) !important; -webkit-text-fill-color: var(--text-color) !important;
        font-weight: bold !important; cursor: not-allowed; background-color: rgba(128, 128, 128, 0.1) !important; border-color: rgba(128, 128, 128, 0.2) !important;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 4. CÁC HÀM XỬ LÝ
# ==========================================
def format_vnd(amount):
    if amount is None: return "0"
    try: return "{:,.0f}".format(float(amount)).replace(",", ".")
    except: return "0"

def extract_numbers_from_line(line):
    raw_nums = re.findall(r'(?<!\d)(?!0\d)\d{1,3}(?:[.,]\d{3})+(?![.,]\d)', line)
    return [float(n.replace('.', '').replace(',', '')) for n in raw_nums if not (1990 <= float(n.replace('.', '').replace(',', '')) <= 2030)]

def extract_pdf_data(uploaded_file, mode="normal"):
    text_content = ""
    try:
        with pdfplumber.open(uploaded_file) as pdf:
            for page in pdf.pages: text_content += (page.extract_text() or "") + "\n"
    except Exception as e: return None, f"Lỗi: {str(e)}"
    
    all_found_numbers = set()
    info = {"date": "", "seller": "", "seller_tax": "", "buyer": "", "inv_num": "", "inv_sym": "", "pre_tax": 0.0, "tax": 0.0, "total": 0.0, "all_numbers": []}
    
    m_no = re.search(r'(?:Số hóa đơn|Số HĐ|Số|No)[:\s\.]*(\d{1,8})\b', text_content, re.IGNORECASE)
    if m_no: info["inv_num"] = m_no.group(1).zfill(7)
    m_sym = re.search(r'(?:Ký hiệu|Mẫu số|Serial)[:\s\.]*([A-Z0-9]{1,2}[A-Z0-9/-]{3,10})', text_content, re.IGNORECASE)
    if m_sym: info["inv_sym"] = m_sym.group(1)
    m_date = re.search(r'(?:Ngày|ngày)\s+(\d{1,2})\s+(?:tháng|Tháng)\s+(\d{1,2})\s+(?:năm|Năm)\s+(\d{4})', text_content)
    if m_date: info["date"] = f"{int(m_date.group(1)):02d}/{int(m_date.group(2)):02d}/{m_date.group(3)}"
    else:
        m_date_alt = re.search(r'(\d{2}/\d{2}/\d{4})', text_content)
        if m_date_alt: info["date"] = m_date_alt.group(1)
    
    lines = text_content.split('\n')
    for line in lines:
        line_l = line.lower()
        nums = extract_numbers_from_line(line)
        for n in nums: all_found_numbers.add(n)
        if not nums: continue
        val = max(nums)
        if any(kw in line_l for kw in ["thanh toán", "tổng cộng"]): info["total"] = val
        elif any(kw in line_l for kw in ["tiền hàng", "thành tiền"]): info["pre_tax"] = val
        elif "thuế" in line_l and "suất" not in line_l: info["tax"] = val
        
    if mode == "deep" or info["total"] == 0:
        all_v = []
        for l in lines: all_v.extend(extract_numbers_from_line(l))
        if all_v: info["total"] = max(all_v)
        
    if info["pre_tax"] == 0: info["pre_tax"] = round(info["total"] / 1.08)
    if info["tax"] == 0: info["tax"] = info["total"] - info["pre_tax"]
    
    for line in lines[:35]:
        l_c = line.strip()
        if re.search(r'^(Đơn vị bán|Người bán|Bên A|Nhà cung cấp)', l_c, re.IGNORECASE): info["seller"] = l_c.split(':')[-1].strip()
        elif re.search(r'^(Đơn vị mua|Người mua|Khách hàng|Bên B)', l_c, re.IGNORECASE): info["buyer"] = l_c.split(':')[-1].strip()
        
    info["all_numbers"] = list(all_found_numbers) 
    return info, None

# ==========================================
# 5. GIAO DIỆN CHÍNH
# ==========================================
if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "user_info" not in st.session_state: st.session_state.user_info = None

if st.session_state.logged_in and st.session_state.user_info is None:
    st.session_state.logged_in = False
    st.rerun()

if not st.session_state.logged_in:
    if "token" in st.query_params:
        try:
            token_str = base64.b64decode(st.query_params["token"]).decode()
            t_user, t_hash = token_str.split(":::")
            sh = get_db()
            ws = safe_get_worksheet(sh, 'users')
            users = safe_get_all_records(ws)
            user_db = next((u for u in users if u['username'] == t_user and u['password'] == t_hash), None)
            if user_db and user_db['status'] == 'approved':
                st.session_state.logged_in = True
                st.session_state.user_info = {"name": user_db['username'], "role": user_db['role']}
                st.rerun()
        except: st.query_params.clear()

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_b:
        st.write("")
        if comp['logo']:
            b64 = base64.b64encode(comp['logo']).decode()
            st.markdown(f'<div style="text-align:center;"><img src="data:image/png;base64,{b64}" height="120" class="header-logo"></div>', unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="text-align:center; margin-top:20px;">
                <h1 style="color:#28a745 !important; margin-bottom:5px;">{comp['name']}</h1>
                <p style="font-size:1.1em;">📍 {comp['address']}<br>📞 {comp['phone']}</p>
            </div>
        """, unsafe_allow_html=True)
        
        tab_login, tab_reg = st.tabs(["🔐 Đăng nhập", "📝 Đăng ký"])
        with tab_login:
            with st.form("login_form"):
                u = st.text_input("Tài khoản")
                p = st.text_input("Mật khẩu", type="password")
                remember = st.checkbox("Lưu thông tin") 
                if st.form_submit_button("XÁC NHẬN ĐĂNG NHẬP", use_container_width=True):
                    sh = get_db()
                    ws = safe_get_worksheet(sh, 'users')
                    if ws:
                        hashed_pw = hash_pass(p)
                        users = safe_get_all_records(ws)
                        user = next((item for item in users if item["username"] == u and item["password"] == hashed_pw), None)
                        if user:
                            if user['status'] == 'approved':
                                st.session_state.logged_in = True
                                st.session_state.user_info = {"name": user['username'], "role": user['role']}
                                if remember:
                                    token_raw = f"{user['username']}:::{hashed_pw}"
                                    token_b64 = base64.b64encode(token_raw.encode()).decode()
                                    st.query_params["token"] = token_b64
                                st.rerun()
                            else: st.error("Tài khoản đang chờ duyệt!")
                        else: st.error("Sai thông tin!")
                    else: st.error("Lỗi kết nối CSDL!")
        with tab_reg:
            with st.form("reg_form"):
                new_u = st.text_input("Tên tài khoản mới")
                new_p = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("GỬI YÊU CẦU", use_container_width=True):
                    if new_u and new_p:
                        try:
                            sh = get_db()
                            ws = safe_get_worksheet(sh, 'users')
                            if ws:
                                users = ws.col_values(2) 
                                if new_u in users: st.error("Tài khoản đã tồn tại!")
                                else:
                                    new_id = get_next_id(ws)
                                    ws.append_row([new_id, new_u, hash_pass(new_p), 'user', 'pending'])
                                    st.success("Đã gửi! Chờ Admin duyệt.")
                        except Exception as e: st.error(f"Lỗi: {e}")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    if comp['logo']:
        b64 = base64.b64encode(comp['logo']).decode()
        st.markdown(f'<div style="text-align:center;"><img src="data:image/png;base64,{b64}" width="150" style="border-radius:10px; margin-bottom:20px;"></div>', unsafe_allow_html=True)
    
    if st.session_state.user_info:
        st.success(f"Chào, **{st.session_state.user_info['name']}**")
        
        with st.container():
            st.markdown("---")
            try:
                sh = get_db()
                if sh: st.markdown("✅ **Database:** Đã kết nối")
                else: st.markdown("❌ **Database:** Lỗi")
            except: pass
            st.markdown("---")
    
    if st.session_state.user_info and st.session_state.user_info['role'] == 'admin':
        with st.expander("⚙️ Quản trị hệ thống"):
            st.subheader("Duyệt thành viên")
            sh = get_db()
            ws_users = safe_get_worksheet(sh, 'users')
            if ws_users:
                u_data = safe_get_all_records(ws_users)
                u_df = pd.DataFrame(u_data)
                if not u_df.empty:
                    u_df = u_df[u_df['role'] == 'user']
                    for _, row in u_df.iterrows():
                        col1, col2 = st.columns([2, 1])
                        col1.write(f"{row['username']} ({row['status']})")
                        if row['status'] == 'pending':
                            if col2.button("Duyệt", key=f"app_{row['id']}"):
                                cell = ws_users.find(str(row['id']), in_column=1)
                                ws_users.update_cell(cell.row, 5, 'approved') 
                                st.rerun()
                        else:
                            if col2.button("Xóa", key=f"delu_{row['id']}"):
                                cell = ws_users.find(str(row['id']), in_column=1)
                                ws_users.delete_rows(cell.row)
                                st.rerun()
            
            st.divider()
            st.subheader("Thông tin Công Ty")
            c_name = st.text_input("Tên Công ty:", value=comp['name'])
            c_addr = st.text_input("Địa chỉ:", value=comp['address'])
            c_phone = st.text_input("SĐT:", value=comp['phone'])
            uploaded_logo = st.file_uploader("Tải Logo mới:", type=['png', 'jpg', 'jpeg'])
            if st.button("💾 Lưu cấu hình", use_container_width=True):
                logo_data = uploaded_logo.read() if uploaded_logo else comp['logo']
                update_company_info(c_name, c_addr, c_phone, logo_data)
                st.success("Đã cập nhật!"); st.rerun()

    if st.button("🚪 Đăng xuất", use_container_width=True):
        st.session_state.logged_in = False
        st.query_params.clear() 
        st.rerun()
    st.divider()
    menu = st.radio("CHỨC NĂNG CHÍNH", ["1. Nhập Hóa Đơn", "2. Liên Kết Dự Án", "3. Báo Cáo Tổng Hợp"])

logo_h = ""
if comp['logo']:
    base64_l = base64.b64encode(comp['logo']).decode()
    logo_h = f'<img src="data:image/png;base64,{base64_l}" height="80" class="header-logo">'
st.markdown(f'<div class="company-header">{logo_h}<div style="text-align: left;"><h1 style="margin:0; color:#28a745 !important;">{comp["name"]}</h1><p style="margin:0;">📍 {comp["address"]} | 📞 {comp["phone"]}</p></div></div>', unsafe_allow_html=True)

if "pdf_data" not in st.session_state: st.session_state.pdf_data = None
if "edit_lock" not in st.session_state: st.session_state.edit_lock = True
if "local_edit_count" not in st.session_state: st.session_state.local_edit_count = 0
if "uploader_key" not in st.session_state: st.session_state.uploader_key = 0

# --- TAB 1: NHẬP HÓA ĐƠN ---
if menu == "1. Nhập Hóa Đơn":
    uploaded_file = st.file_uploader("📤 Kéo thả file hóa đơn PDF vào đây", type=["pdf"], key=f"up_{st.session_state.uploader_key}")
    
    show_pdf = st.checkbox("👁️ Hiển thị file PDF", value=True)
    
    if uploaded_file:
        if show_pdf:
            col_pdf, col_form = st.columns([1, 1])
        else:
            col_pdf = None
            col_form = st.container()

        if show_pdf and col_pdf:
            with col_pdf:
                try:
                    with pdfplumber.open(uploaded_file) as pdf:
                        st.info(f"📄 File có {len(pdf.pages)} trang:")
                        for i, page in enumerate(pdf.pages):
                            im = page.to_image(resolution=150)
                            st.image(im.original, caption=f"Trang {i+1}", use_container_width=True)
                except Exception as e:
                    st.error(f"Lỗi hiển thị preview: {e}")
                    st.download_button("📥 Tải PDF về xem", data=uploaded_file.getvalue(), file_name=uploaded_file.name)
        
        with col_form:
            if st.button("🔍 Bước 2: PHÂN TÍCH FILE", type="primary", use_container_width=True):
                data, _ = extract_pdf_data(uploaded_file)
                data['file_name'] = uploaded_file.name 
                st.session_state.pdf_data = data; st.session_state.edit_lock = True; st.session_state.local_edit_count = 0
                
                calc = data['pre_tax'] + data['tax']
                diff = abs(data['total'] - calc)
                if diff < 10: 
                    st.success(f"✅ Tiền nong chuẩn chỉ! (Tổng: {format_vnd(data['total'])})")
                else: 
                    st.warning(f"⚠️ Cảnh báo lệch tiền: {format_vnd(diff)}đ")

            if st.session_state.pdf_data:
                data = st.session_state.pdf_data
                all_nums = data.get('all_numbers', [])

                def check_exist(val):
                    if val in all_nums: return "✅ Có trong file"
                    return "⚠️ Không tìm thấy!"

                with st.form("invoice_form"):
                    inv_t = st.radio("Loại:", ["Đầu vào", "Đầu ra"], horizontal=True)
                    memo = st.text_input("📝 Tên gợi nhớ:", value=data.get('file_name', ''), placeholder="Ví dụ: Tiền cát, Tiếp khách...")
                    i_date = st.text_input("Ngày HĐ", value=data['date'])
                    cn, cs = st.columns(2)
                    with cn: i_num = st.text_input("Số HĐ", value=data['inv_num'])
                    with cs: i_sym = st.text_input("Ký hiệu", value=data['inv_sym'])
                    st.divider()
                    seller = st.text_input("Bên Bán", value=data['seller'])
                    buyer = st.text_input("Bên Mua", value=data['buyer'])
                    
                    st.markdown("#### 💰 Kiểm tra Tiền")
                    
                    new_pre = st.number_input("Tiền hàng", value=float(data['pre_tax']), disabled=st.session_state.edit_lock, format="%.0f")
                    if not st.session_state.edit_lock: st.caption(check_exist(new_pre))

                    new_tax = st.number_input("VAT", value=float(data['tax']), disabled=st.session_state.edit_lock, format="%.0f")
                    if not st.session_state.edit_lock: st.caption(check_exist(new_tax))
                    
                    total_c = new_pre + new_tax
                    
                    if st.session_state.local_edit_count == 1:
                        st.markdown('<div style="background-color:#ffeef7; color:#000000; padding:10px; border-radius:5px; margin-bottom:10px; border: 1px solid #f8d7da;">🌸 <b>Lần sửa 1/2:</b> Cẩn thận nha bé ơi!</div>', unsafe_allow_html=True)
                    elif st.session_state.local_edit_count == 2:
                        st.markdown('<div style="background-color:#fff3cd; color:#000000; padding:10px; border-radius:5px; margin-bottom:10px; border: 1px solid #ffeeba;">🍊 <b>Lần sửa 2/2:</b> Hết lượt sửa rồi đó!</div>', unsafe_allow_html=True)

                    is_match = abs(data['total'] - total_c) < 10
                    match_txt = "(Khớp lệnh! ✅)" if is_match else "(Chưa khớp đâu 🥺)"
                    st.markdown(f'<div class="money-box" style="text-align:center;">Tổng tính toán: <b>{format_vnd(total_c)}</b><br><span style="font-size:0.8em; color:white;">{match_txt}</span></div>', unsafe_allow_html=True)
                    
                    if not st.session_state.edit_lock:
                        if "✅" in check_exist(total_c): st.success(f"Tổng tiền khớp trong file PDF.")
                        else: st.warning(f"Lưu ý: Tổng tiền không tìm thấy trong file.")

                    c1, c2 = st.columns(2)
                    with c1:
                        if st.form_submit_button("✏️ Chỉnh sửa giá"):
                            if st.session_state.local_edit_count >= 2: st.error("🚫 Hết lượt chỉnh sửa rồi!")
                            else: st.session_state.edit_lock = False; st.rerun()
                    with c2:
                        if not st.session_state.edit_lock:
                            if st.form_submit_button("✅ Xác nhận khớp giá"):
                                st.session_state.pdf_data['pre_tax'] = new_pre
                                st.session_state.pdf_data['tax'] = new_tax
                                st.session_state.pdf_data['total'] = total_c 
                                st.session_state.edit_lock = True
                                st.session_state.local_edit_count += 1
                                st.rerun()

                    # --- LƯU DỮ LIỆU & UPLOAD DRIVE (AUTO BYPASS QUOTA) ---
                    if st.form_submit_button("💾 LƯU DỮ LIỆU", type="primary", use_container_width=True):
                        if not i_date or not i_num or not i_sym: st.error("Úi, thiếu thông tin rồi! 🥺")
                        elif not st.session_state.edit_lock: st.warning("Bấm nút 'Xác nhận khớp giá' trước đã! 🔒")
                        else:
                            with st.spinner('Đang xử lý...'):
                                # 1. Upload Drive (Thử vận may)
                                drive_link = ""
                                drive_msg = ""
                                if uploaded_file:
                                    uploaded_file.seek(0)
                                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    final_filename = f"{ts}_{uploaded_file.name}"
                                    link, err_msg = upload_to_drive(uploaded_file, final_filename)
                                    
                                    if link: 
                                        drive_link = link
                                        drive_msg = "✅ Upload Drive OK"
                                    elif err_msg == "QUOTA_ERROR":
                                        drive_msg = "⚠️ Tài khoản Gmail cá nhân không hỗ trợ Robot Upload (Bỏ qua file)"
                                    else:
                                        drive_msg = f"⚠️ Lỗi Drive: {err_msg}"

                                # 2. Lưu Sheet (Quan trọng nhất)
                                try:
                                    sh = get_db()
                                    ws = safe_get_worksheet(sh, 'invoices')
                                    new_id = get_next_id(ws)
                                    row_data = [new_id, 'OUT' if "Đầu ra" in inv_t else 'IN', '', i_date, i_num, i_sym, seller, '', buyer, new_pre, new_tax, total_c, '', 'active', st.session_state.local_edit_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), memo, drive_link]
                                    ws.append_row(row_data)
                                    
                                    # Thông báo kết quả
                                    st.success("Đã lưu dữ liệu vào Sheet thành công! 🎉")
                                    if drive_msg: st.info(drive_msg)
                                    
                                    time.sleep(2)
                                    st.session_state.pdf_data = None; st.session_state.uploader_key += 1; st.rerun()
                                except Exception as e: st.error(f"Lỗi lưu Sheet: {e}")

    st.divider()
    with st.expander("🗑️ Lịch sử & Hủy Hóa Đơn", expanded=True):
        sh = get_db()
        ws = safe_get_worksheet(sh, 'invoices')
        if ws:
            data = safe_get_all_records(ws)
            df = pd.DataFrame(data)
            if not df.empty:
                df = df.sort_values(by='id', ascending=False).head(15)
                df['Tiền'] = df['total_amount'].apply(format_vnd)
                df['Trạng thái sửa'] = df['edit_count'].apply(lambda x: f"⚠️ Sửa {x} lần" if x > 0 else "Gốc")

                def style_table(row):
                    if row.get('status') == 'deleted': return ['background-color: #5c0e0e; color: #ff9999'] * len(row)
                    try:
                        ec = row['edit_count']
                        if ec == 1: return ['background-color: #ffeef7; color: #000000'] * len(row) 
                        elif ec >= 2: return ['background-color: #fff3cd; color: #000000'] * len(row)
                    except: pass
                    return [''] * len(row)
                
                cols_show = ['id', 'type', 'memo', 'invoice_number', 'Tiền', 'status', 'drive_url', 'Trạng thái sửa', 'edit_count']
                st.dataframe(
                    df[cols_show].style.apply(style_table, axis=1), 
                    column_config={
                        "drive_url": st.column_config.LinkColumn("File", display_text="Xem"),
                        "edit_count": None
                    },
                    use_container_width=True
                )
                
                if st.session_state.user_info['role'] == 'admin':
                    a_ids = df[df['status'] == 'active']['id'].tolist()
                    if a_ids:
                        c_s, c_b = st.columns([3, 1])
                        d_id = c_s.selectbox("ID hủy:", a_ids)
                        if c_b.button("❌ Hủy", type="primary"):
                            cell = ws.find(str(d_id), in_column=1)
                            ws.update_cell(cell.row, 14, 'deleted')
                            st.rerun()

# --- TAB 2: LIÊN KẾT DỰ ÁN ---
elif menu == "2. Liên Kết Dự Án":
    sh = get_db()
    ws_proj = safe_get_worksheet(sh, 'projects')
    if ws_proj:
        projs = safe_get_all_records(ws_proj)
        df_projs = pd.DataFrame(projs)
        
        st.subheader("📁 Quản Lý Dự Án")
        c_list, c_act = st.columns([2, 1])
        with c_list:
            p_opts = {r['project_name']: r['id'] for _, r in df_projs.iterrows()} if not df_projs.empty else {}
            sel_p = st.selectbox("Chọn Dự Án:", list(p_opts.keys()) if p_opts else [], key="main_p")

        with c_act:
            with st.popover("➕ Thêm / 🗑️ Xóa"):
                with st.form("cr_p", clear_on_submit=True):
                    np = st.text_input("Tên dự án mới")
                    if st.form_submit_button("Tạo"):
                        if np:
                            nid = get_next_id(ws_proj)
                            ws_proj.append_row([nid, np, datetime.now().strftime("%Y-%m-%d")])
                            st.rerun()
                if p_opts:
                    del_p = st.selectbox("Xóa dự án:", list(p_opts.keys()))
                    if st.button("Xóa"):
                        if st.session_state.user_info['role'] == 'admin':
                            pid = p_opts[del_p]
                            cell = ws_proj.find(str(pid), in_column=1)
                            ws_proj.delete_rows(cell.row)
                            st.rerun()
                        else: st.error("Cần quyền Admin")

        if sel_p:
            pid = p_opts[sel_p]
            if "edit_mode" not in st.session_state: st.session_state.edit_mode = False
            if not st.session_state.edit_mode:
                if st.button("✏️ Mở Khóa Liên Kết"): st.session_state.edit_mode = True; st.rerun()
            else:
                if st.button("💾 LƯU THAY ĐỔI", type="primary"): st.session_state.trigger_save = True

            ws_links = safe_get_worksheet(sh, 'project_links')
            links = safe_get_all_records(ws_links)
            ws_inv = safe_get_worksheet(sh, 'invoices')
            invs = safe_get_all_records(ws_inv)
            df_invs = pd.DataFrame(invs)
            
            if not df_invs.empty:
                df_invs = df_invs[df_invs['status'] == 'active'].sort_values(by='date', ascending=False)
                mine = [l['invoice_id'] for l in links if l['project_id'] == pid]
                blocked = [l['invoice_id'] for l in links if l['project_id'] != pid]
                avail = df_invs[~df_invs['id'].isin(blocked)].copy()
                
                avail['Selected'] = avail['id'].isin(mine)
                avail['Money'] = avail['total_amount'].apply(format_vnd)
                avail['Name'] = avail['memo'].fillna('') + " (" + avail['invoice_number'].astype(str) + ")"
                
                c1, c2 = st.columns(2)
                disabled = not st.session_state.edit_mode
                
                with c1:
                    st.warning("Đầu vào")
                    df_in = avail[avail['type'] == 'IN'][['Selected', 'id', 'Name', 'Money']]
                    ed_in = st.data_editor(df_in, column_config={"Selected": st.column_config.CheckboxColumn(required=True), "id": None}, disabled=["Name", "Money"] if not disabled else ["Selected", "Name", "Money"], hide_index=True, key="edin")
                with c2:
                    st.info("Đầu ra")
                    df_out = avail[avail['type'] == 'OUT'][['Selected', 'id', 'Name', 'Money']]
                    ed_out = st.data_editor(df_out, column_config={"Selected": st.column_config.CheckboxColumn(required=True), "id": None}, disabled=["Name", "Money"] if not disabled else ["Selected", "Name", "Money"], hide_index=True, key="edout")

                if st.session_state.get("trigger_save"):
                    ids = []
                    if not ed_in.empty: ids.extend(ed_in[ed_in['Selected']]['id'].tolist())
                    if not ed_out.empty: ids.extend(ed_out[ed_out['Selected']]['id'].tolist())
                    
                    # Xóa cũ
                    all_l = safe_get_all_records(ws_links)
                    to_del = [i+2 for i, l in enumerate(all_l) if l['project_id'] == pid]
                    for r in sorted(to_del, reverse=True): ws_links.delete_rows(r)
                    
                    # Thêm mới
                    if ids:
                        nid = get_next_id(ws_links)
                        new_r = [[nid+i, pid, iid] for i, iid in enumerate(ids)]
                        ws_links.append_rows(new_r)
                    
                    st.session_state.edit_mode = False
                    st.session_state.trigger_save = False
                    st.rerun()

elif menu == "3. Báo Cáo Tổng Hợp":
    st.title("📊 Báo Cáo Tài Chính")
    sh = get_db()
    if sh:
        df_p = pd.DataFrame(safe_get_all_records(safe_get_worksheet(sh, 'projects')))
        df_l = pd.DataFrame(safe_get_all_records(safe_get_worksheet(sh, 'project_links')))
        df_i = pd.DataFrame(safe_get_all_records(safe_get_worksheet(sh, 'invoices')))

        if not df_p.empty and not df_l.empty and not df_i.empty:
            m = pd.merge(df_p, df_l, left_on='id', right_on='project_id', suffixes=('_p', '_l'))
            m = pd.merge(m, df_i, left_on='invoice_id', right_on='id')
            m = m[m['status'] == 'active']
            
            if not m.empty:
                m['date_dt'] = pd.to_datetime(m['date'], format='%d/%m/%Y', errors='coerce')
                m['Month'] = m['date_dt'].dt.strftime('%m/%Y')
                
                agg = m.groupby(['project_name', 'type'])['total_amount'].sum().unstack(fill_value=0).reset_index()
                if 'IN' not in agg: agg['IN'] = 0
                if 'OUT' not in agg: agg['OUT'] = 0
                agg['Lãi'] = agg['OUT'] - agg['IN']
                
                last_date = m.groupby('project_name')['date_dt'].max().reset_index()
                agg = pd.merge(agg, last_date, on='project_name').sort_values('date_dt', ascending=False)

                st.metric("TỔNG DOANH THU", format_vnd(agg['OUT'].sum()))
                st.divider()
                
                for _, r in agg.iterrows():
                    with st.container():
                        st.markdown(f"""
                        <div class="report-card">
                            <h4>📂 {r['project_name']}</h4>
                            <hr style="margin: 5px 0;">
                            <div style="display:flex; justify-content:space-between;">
                                <div>Thu: <b>{format_vnd(r['OUT'])}</b></div>
                                <div>Chi: <b>{format_vnd(r['IN'])}</b></div>
                                <div style="color:{'#28a745' if r['Lãi']>=0 else 'red'}">Lãi: <b>{format_vnd(r['Lãi'])}</b></div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
            else: st.info("Chưa có dữ liệu.")
        else: st.info("Chưa có dữ liệu.")
