import streamlit as st
import pandas as pd
import sqlite3
import pdfplumber
import re
from datetime import datetime
import time
import base64
import hashlib
from io import BytesIO

# ==========================================
# 1. CẤU HÌNH TRANG
# ==========================================
st.set_page_config(page_title="Quản Lý Hóa Đơn Pro", page_icon="📑", layout="wide")

DB_FILE = 'invoice_data_pdf.db'

# ==========================================
# 2. DATABASE
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    
    # Bảng users
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        username TEXT UNIQUE, password TEXT, role TEXT, status TEXT
    )''')
    try: c.execute("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'approved'")
    except: pass

    # Admin mặc định
    admin_pw = hashlib.sha256("admin123".encode()).hexdigest()
    c.execute("INSERT OR IGNORE INTO users (username, password, role, status) VALUES ('Admin', ?, 'admin', 'approved')", (admin_pw,))

    # Bảng hóa đơn
    c.execute('''CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT, group_name TEXT, date TEXT, 
        invoice_number TEXT, invoice_symbol TEXT,
        seller_name TEXT, seller_tax TEXT, buyer_name TEXT,
        pre_tax_amount REAL, tax_amount REAL, total_amount REAL,
        file_name TEXT, status TEXT DEFAULT 'active',
        edit_count INTEGER DEFAULT 0, 
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        memo TEXT
    )''')
    try: c.execute("ALTER TABLE invoices ADD COLUMN memo TEXT")
    except: pass
    
    # Bảng dự án & liên kết
    c.execute('''CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT, project_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS project_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, invoice_id INTEGER,
        FOREIGN KEY(project_id) REFERENCES projects(id), FOREIGN KEY(invoice_id) REFERENCES invoices(id)
    )''')

    # Bảng thông tin công ty
    c.execute('''CREATE TABLE IF NOT EXISTS company_info (
        id INTEGER PRIMARY KEY, name TEXT, address TEXT, phone TEXT, logo BLOB,
        bg_color TEXT, text_color TEXT, box_color TEXT
    )''')
    c.execute("INSERT OR IGNORE INTO company_info (id, name, address, phone) VALUES (1, 'Tên Công Ty Của Bé', 'Địa chỉ', 'SĐT')")
    
    conn.commit()
    conn.close()

init_db()

def hash_pass(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def get_company_data():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    try:
        df = pd.read_sql("SELECT * FROM company_info WHERE id=1", conn)
    except:
        init_db()
        df = pd.read_sql("SELECT * FROM company_info WHERE id=1", conn)
    conn.close()
    return df.iloc[0] if not df.empty else None

def update_company_info(name, address, phone, logo_bytes=None):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    if logo_bytes:
        c.execute("UPDATE company_info SET name=?, address=?, phone=?, logo=? WHERE id=1", 
                  (name, address, phone, logo_bytes))
    else:
        c.execute("UPDATE company_info SET name=?, address=?, phone=? WHERE id=1", 
                  (name, address, phone))
    conn.commit()
    conn.close()

# ==========================================
# 3. CSS ĐỘNG & XỬ LÝ GIAO DIỆN
# ==========================================
comp = get_company_data()

st.markdown("""
<style>
    /* 1. Thiết lập chung */
    .stApp { 
        background-color: var(--background-color);
        color: var(--text-color);
        font-family: 'Segoe UI', sans-serif;
    }
    
    /* 2. Box tiền */
    .money-box { 
        background: linear-gradient(135deg, #1e7e34 0%, #28a745 100%) !important;
        color: #ffffff !important;
        padding: 20px; 
        border-radius: 12px; 
        box-shadow: 0 4px 15px rgba(40, 167, 69, 0.4); 
        font-size: 1.2em;
        font-weight: bold;
        text-align: center;
        border: none;
    }
    
    /* 3. Card báo cáo */
    .report-card, .login-container { 
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 12px; 
        padding: 20px; 
        margin-bottom: 15px; 
        color: var(--text-color) !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        transition: transform 0.2s; 
    }
    .report-card:hover { 
        transform: translateY(-3px); 
        border-color: #28a745; 
        box-shadow: 0 6px 12px rgba(40, 167, 69, 0.2);
    }
    
    .stButton button { 
        border-radius: 8px; 
        font-weight: 600; 
        text-transform: uppercase; 
        letter-spacing: 0.5px; 
        transition: all 0.3s; 
    }
    
    /* 4. Header công ty */
    .company-header { 
        display: flex; 
        align-items: center; 
        justify-content: center; 
        gap: 25px; 
        margin-bottom: 30px; 
        border-bottom: 1px solid rgba(128, 128, 128, 0.2);
        padding-bottom: 20px; 
        background: transparent;
        padding: 20px; 
    }
    .header-logo { border-radius: 10px; object-fit: contain; }
    
    /* 5. Màn hình đăng nhập */
    .login-container { 
        max-width: 500px; 
        margin: 0 auto; 
        padding: 40px; 
        text-align: center; 
    }
    
    .time-badge { 
        background-color: var(--secondary-background-color); 
        color: #28a745; 
        padding: 4px 12px; 
        border-radius: 20px; 
        font-size: 0.85em; 
        font-weight: bold; 
        border: 1px solid #28a745; 
    }
    iframe { border-radius: 10px; border: 1px solid rgba(128, 128, 128, 0.2); }

    h1, h2, h3, h4, h5, p, span, div, label { color: var(--text-color) !important; }
    .stAlert p, .stAlert div, .stAlert h4, .stAlert span { color: inherit !important; }
    .money-box b, .money-box div { color: #ffffff !important; }
    
    /* 7. Ô NHẬP LIỆU */
    .stTextInput input, .stNumberInput input { 
        color: var(--text-color) !important; 
        background-color: var(--secondary-background-color) !important;
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 8px;
    }
    .stTextInput input:focus, .stNumberInput input:focus {
        border-color: #28a745 !important;
        box-shadow: 0 0 0 1px #28a745;
    }
    
    /* 8. DISABLE INPUT */
    input:disabled, 
    div[data-testid="stNumberInput"] input[disabled], 
    div[data-testid="stTextInput"] input[disabled] {
        opacity: 1 !important;
        color: var(--text-color) !important;
        -webkit-text-fill-color: var(--text-color) !important;
        font-weight: bold !important;
        cursor: not-allowed;
        background-color: rgba(128, 128, 128, 0.1) !important;
        border-color: rgba(128, 128, 128, 0.2) !important;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 4. CÁC HÀM XỬ LÝ
# ==========================================
def format_vnd(amount):
    if amount is None: return "0"
    return "{:,.0f}".format(amount).replace(",", ".")

def extract_numbers_from_line(line):
    raw_nums = re.findall(r'(?<!\d)(?!0\d)\d{1,3}(?:[.,]\d{3})+(?![.,]\d)', line)
    return [float(n.replace('.', '').replace(',', '')) for n in raw_nums if not (1990 <= float(n.replace('.', '').replace(',', '')) <= 2030)]

def extract_pdf_data(uploaded_file, mode="normal"):
    text_content = ""
    try:
        with pdfplumber.open(uploaded_file) as pdf:
            for page in pdf.pages: text_content += (page.extract_text() or "") + "\n"
    except Exception as e: return None, f"Lỗi: {str(e)}"
    info = {"date": "", "seller": "", "seller_tax": "", "buyer": "", "inv_num": "", "inv_sym": "", "pre_tax": 0.0, "tax": 0.0, "total": 0.0}
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
            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            c = conn.cursor()
            c.execute("SELECT username, role, status FROM users WHERE username=? AND password=?", (t_user, t_hash))
            user_db = c.fetchone()
            conn.close()
            if user_db and user_db[2] == 'approved':
                st.session_state.logged_in = True
                st.session_state.user_info = {"name": user_db[0], "role": user_db[1]}
                st.rerun()
        except:
            st.query_params.clear()

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_b:
        st.write("")
        logo_img = ""
        if comp['logo']:
            b64 = base64.b64encode(comp['logo']).decode()
            st.markdown(f'<div style="text-align:center;"><img src="data:image/png;base64,{b64}" height="120" class="header-logo"></div>', unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="text-align:center; margin-top:20px;">
                <h1 style="color:#28a745 !important; margin-bottom:5px;">{comp['name']}</h1>
                <p style="font-size:1.1em;">📍 {comp['address']}<br>📞 {comp['phone']}</p>
            </div>
        """, unsafe_allow_html=True)
        
        tab_login, tab_reg = st.tabs(["🔐 Đăng nhập hệ thống", "📝 Đăng ký nội bộ"])
        
        with tab_login:
            with st.form("login_form"):
                u = st.text_input("Tài khoản")
                p = st.text_input("Mật khẩu", type="password")
                remember = st.checkbox("Lưu thông tin đăng nhập") 
                
                if st.form_submit_button("XÁC NHẬN ĐĂNG NHẬP", use_container_width=True):
                    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
                    c = conn.cursor()
                    hashed_pw = hash_pass(p)
                    c.execute("SELECT username, role, status FROM users WHERE username=? AND password=?", (u, hashed_pw))
                    user = c.fetchone()
                    conn.close()
                    if user:
                        if user[2] == 'approved':
                            st.session_state.logged_in = True
                            st.session_state.user_info = {"name": user[0], "role": user[1]}
                            if remember:
                                token_raw = f"{user[0]}:::{hashed_pw}"
                                token_b64 = base64.b64encode(token_raw.encode()).decode()
                                st.query_params["token"] = token_b64
                            st.rerun()
                        else: st.error("Tài khoản đang chờ duyệt!")
                    else: st.error("Sai thông tin đăng nhập!")

        with tab_reg:
            with st.form("reg_form"):
                new_u = st.text_input("Tên tài khoản mới")
                new_p = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("GỬI YÊU CẦU ĐĂNG KÝ", use_container_width=True):
                    if new_u and new_p:
                        try:
                            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
                            c = conn.cursor()
                            c.execute("INSERT INTO users (username, password, role, status) VALUES (?, ?, 'user', 'pending')", (new_u, hash_pass(new_p)))
                            conn.commit(); conn.close()
                            st.success("Đã gửi! Chờ Admin duyệt nhé bé.")
                        except: st.error("Tài khoản đã tồn tại!")
    st.stop()

# --- SIDEBAR & ADMIN PANEL ---
with st.sidebar:
    if comp['logo']:
        b64 = base64.b64encode(comp['logo']).decode()
        st.markdown(f'<div style="text-align:center;"><img src="data:image/png;base64,{b64}" width="150" style="border-radius:10px; margin-bottom:20px;"></div>', unsafe_allow_html=True)
    
    if st.session_state.user_info:
        st.success(f"Chào, **{st.session_state.user_info['name']}**")
    
    if st.session_state.user_info and st.session_state.user_info['role'] == 'admin':
        with st.expander("⚙️ Quản trị hệ thống"):
            st.subheader("Duyệt thành viên")
            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            u_df = pd.read_sql("SELECT id, username, status FROM users WHERE role='user'", conn)
            for _, row in u_df.iterrows():
                col1, col2 = st.columns([2, 1])
                col1.write(f"{row['username']} ({row['status']})")
                if row['status'] == 'pending':
                    if col2.button("Duyệt ✅", key=f"app_{row['id']}"):
                        conn.execute("UPDATE users SET status='approved' WHERE id=?", (row['id'],)); conn.commit(); st.rerun()
                else:
                    if col2.button("Xóa 🗑️", key=f"delu_{row['id']}"):
                        conn.execute("DELETE FROM users WHERE id=?", (row['id'],)); conn.commit(); st.rerun()
            conn.close()
            
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

            st.divider()
            st.subheader("⚠️ Quản lý dữ liệu (Nguy hiểm)")
            with st.popover("🗑️ XÓA TOÀN BỘ HÓA ĐƠN"):
                st.warning("CẢNH BÁO: Hành động này sẽ xóa sạch toàn bộ hóa đơn và liên kết dự án! Không thể hoàn tác.")
                confirm_del = st.text_input("Nhập 'DELETE' để xác nhận:", key="admin_reset_confirm")
                if st.button("XÁC NHẬN XÓA SẠCH", type="primary", disabled=(confirm_del != "DELETE")):
                    try:
                        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
                        conn.execute("DELETE FROM invoices")
                        conn.execute("DELETE FROM project_links")
                        conn.execute("DELETE FROM sqlite_sequence WHERE name='invoices'")
                        conn.execute("DELETE FROM sqlite_sequence WHERE name='project_links'")
                        conn.commit()
                        conn.close()
                        st.success("Đã xóa toàn bộ dữ liệu!")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Lỗi: {str(e)}")
    
    if st.button("🚪 Đăng xuất", use_container_width=True):
        st.session_state.logged_in = False
        st.query_params.clear() 
        st.rerun()
    st.divider()
    menu = st.radio("CHỨC NĂNG CHÍNH", ["1. Nhập Hóa Đơn", "2. Liên Kết Dự Án", "3. Báo Cáo Tổng Hợp"])

# Nội dung Header chính
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
    
    show_pdf = st.checkbox("👁️ Hiển thị file PDF (Bật/Tắt)", value=True)
    
    if uploaded_file:
        if show_pdf:
            col_pdf, col_form = st.columns([1, 1])
        else:
            col_pdf = None
            col_form = st.container()

        if show_pdf and col_pdf:
            with col_pdf:
                b64_pdf = base64.b64encode(uploaded_file.getvalue()).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_pdf}" width="100%" height="800"></iframe>', unsafe_allow_html=True)
        
        with col_form:
            if st.button("🔍 Bước 2: PHÂN TÍCH FILE", type="primary", use_container_width=True):
                data, _ = extract_pdf_data(uploaded_file)
                # LẤY TÊN FILE LÀM TÊN GỢI NHỚ MẶC ĐỊNH
                data['file_name'] = uploaded_file.name 
                st.session_state.pdf_data = data; st.session_state.edit_lock = True; st.session_state.local_edit_count = 0
                
                # CHECK TIỀN NGAY KHI SOI
                calc = data['pre_tax'] + data['tax']
                diff = abs(data['total'] - calc)
                if diff < 10: 
                    st.success(f"✅ Tiền nong chuẩn chỉ! Tuyệt vời ông mặt trời 🌞 (Tổng: {format_vnd(data['total'])})")
                else: 
                    st.warning(f"⚠️ Ôi không, tiền bị lệch {format_vnd(diff)}đ rồi! bé kiểm tra lại nha 🧐💸 (File: {format_vnd(data['total'])} - Máy tính: {format_vnd(calc)})")

            if st.session_state.pdf_data:
                data = st.session_state.pdf_data
                
                # --- PHẦN FORM NHẬP LIỆU ---
                with st.form("invoice_form"):
                    inv_t = st.radio("Loại:", ["Đầu vào", "Đầu ra"], horizontal=True)
                    # SỬ DỤNG TÊN FILE LÀM GIÁ TRỊ MẶC ĐỊNH
                    memo = st.text_input("📝 Tên gợi nhớ:", value=data.get('file_name', ''), placeholder="Ví dụ: Tiền cát, Tiếp khách...")
                    
                    i_date = st.text_input("Ngày HĐ", value=data['date'])
                    cn, cs = st.columns(2)
                    with cn: i_num = st.text_input("Số HĐ", value=data['inv_num'])
                    with cs: i_sym = st.text_input("Ký hiệu", value=data['inv_sym'])
                    st.divider()
                    seller = st.text_input("Bên Bán", value=data['seller'])
                    buyer = st.text_input("Bên Mua", value=data['buyer'])
                    
                    # Ô nhập tiền
                    new_pre = st.number_input("Tiền hàng", value=float(data['pre_tax']), disabled=st.session_state.edit_lock, format="%.0f")
                    new_tax = st.number_input("VAT", value=float(data['tax']), disabled=st.session_state.edit_lock, format="%.0f")
                    
                    # Tự động cộng lại tiền khi render
                    total_c = new_pre + new_tax
                    
                    # CẢNH BÁO SỐ LẦN SỬA (MỚI THÊM)
                    if st.session_state.local_edit_count == 1:
                        st.markdown('<div style="background-color:#ffeef7; color:#d63384; padding:10px; border-radius:5px; margin-bottom:10px; border: 1px solid #f8d7da;">🌸 <b>Lần sửa 1/2:</b> Cẩn thận nha bé ơi! Sắp hết lượt rồi đó.</div>', unsafe_allow_html=True)
                    elif st.session_state.local_edit_count == 2:
                        st.markdown('<div style="background-color:#fff3cd; color:#856404; padding:10px; border-radius:5px; margin-bottom:10px; border: 1px solid #ffeeba;">🍊 <b>Lần sửa 2/2:</b> Hết lượt sửa rồi đó nha! Kiểm tra kỹ trước khi lưu nhé.</div>', unsafe_allow_html=True)

                    # BOX TỔNG TIỀN VỚI TRẠNG THÁI CHECK KHỚP
                    is_match = abs(data['total'] - total_c) < 10
                    match_txt = "(Khớp lệnh! ✅)" if is_match else "(Chưa khớp đâu 🥺)"
                    st.markdown(f'<div class="money-box" style="text-align:center;">Tổng tính toán: <b>{format_vnd(total_c)}</b><br><span style="font-size:0.8em; color:white;">{match_txt}</span></div>', unsafe_allow_html=True)
                    
                    # --- NÚT ĐIỀU KHIỂN ---
                    c1, c2 = st.columns(2)
                    with c1:
                        # Nút mở khóa sửa - CÓ GIỚI HẠN 2 LẦN
                        if st.form_submit_button("✏️ Chỉnh sửa giá"):
                            if st.session_state.local_edit_count >= 2:
                                st.error("🚫 Hết lượt chỉnh sửa rồi bé ơi! (Quy định max 2 lần thui)")
                            else:
                                st.session_state.edit_lock = False; st.rerun()
                    with c2:
                        # Nút Xác nhận khớp giá - CHỈ HIỆN KHI ĐANG MỞ KHÓA SỬA
                        if not st.session_state.edit_lock:
                            if st.form_submit_button("✅ Xác nhận khớp giá"):
                                # Check cộng lại tiền: Cập nhật lại total trong session_state data để đảm bảo nhất quán
                                st.session_state.pdf_data['pre_tax'] = new_pre
                                st.session_state.pdf_data['tax'] = new_tax
                                st.session_state.pdf_data['total'] = total_c # Tổng tiền = Tiền hàng + VAT
                                st.session_state.edit_lock = True
                                st.session_state.local_edit_count += 1
                                st.rerun()

                    if st.form_submit_button("💾 LƯU DỮ LIỆU", type="primary", use_container_width=True):
                        if not i_date or not i_num or not i_sym: st.error("Úi, bé quên nhập thông tin rồi! 🥺")
                        elif not st.session_state.edit_lock: st.warning("Bấm nút 'Xác nhận khớp giá' để chốt đơn đã nhé! 🔒✨")
                        else:
                            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
                            # Lưu total_c (đã cộng lại) thay vì data['total'] cũ
                            conn.execute('INSERT INTO invoices (type, date, invoice_number, invoice_symbol, seller_name, buyer_name, pre_tax_amount, tax_amount, total_amount, edit_count, status, memo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                                         ('OUT' if "Đầu ra" in inv_t else 'IN', i_date, i_num, i_sym, seller, buyer, new_pre, new_tax, total_c, st.session_state.local_edit_count, 'active', memo))
                            conn.commit(); conn.close(); st.session_state.pdf_data = None; st.session_state.uploader_key += 1; st.rerun()

    st.divider()
    with st.expander("🗑️ Lịch sử & Hủy Hóa Đơn", expanded=True):
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        # THÊM CỘT edit_count VÀO SQL
        df = pd.read_sql("SELECT id, type, memo, invoice_number, total_amount, status, edit_count FROM invoices ORDER BY id DESC LIMIT 15", conn)
        if not df.empty:
            df['Tiền'] = df['total_amount'].apply(format_vnd)
            # THÊM CỘT CẢNH BÁO SỬA
            df['Trạng thái sửa'] = df['edit_count'].apply(lambda x: f"⚠️ Sửa {x} lần" if x > 0 else "Gốc")

            def style_table(row):
                # Ưu tiên màu xóa trước
                if row.status == 'deleted': return ['background-color: #5c0e0e; color: #ff9999'] * len(row)
                
                # Cảnh báo sửa trong lịch sử
                if row['edit_count'] == 1:
                    return ['background-color: #ffeef7; color: #d63384'] * len(row) # Màu hồng
                elif row['edit_count'] >= 2:
                    return ['background-color: #fff3cd; color: #856404'] * len(row) # Màu cam
                
                return [''] * len(row)
            
            st.dataframe(df.style.apply(style_table, axis=1), use_container_width=True)
            
            if st.session_state.user_info['role'] == 'admin':
                a_ids = df[df['status'] == 'active']['id'].tolist()
                if a_ids:
                    c_s, c_b = st.columns([3, 1])
                    d_id = c_s.selectbox("ID cần hủy:", a_ids)
                    if c_b.button("❌ Hủy", type="primary"):
                        conn.execute("UPDATE invoices SET status='deleted' WHERE id=?", (d_id,))
                        conn.execute("DELETE FROM project_links WHERE invoice_id=?", (d_id,))
                        conn.commit(); st.rerun()
        conn.close()

# --- TAB 2: LIÊN KẾT DỰ ÁN ---
elif menu == "2. Liên Kết Dự Án":
    if "edit_mode" not in st.session_state: st.session_state.edit_mode = False
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    
    # Khu vực Quản lý Dự Án
    st.subheader("📁 Quản Lý Dự Án")
    c_list, c_act = st.columns([2, 1])
    
    with c_list:
        projs = pd.read_sql("SELECT * FROM projects ORDER BY id DESC", conn)
        p_opts = {r['project_name']: r['id'] for _, r in projs.iterrows()}
        sel_p = st.selectbox("Chọn Dự Án làm việc:", list(p_opts.keys()) if p_opts else [], key="main_project_select")

    with c_act:
        # Form Tạo dự án mới (Dùng st.form clear_on_submit để sửa lỗi crash)
        with st.popover("➕ Thêm / 🗑️ Xóa Dự án"):
            st.markdown("### Tạo mới")
            with st.form("create_proj_form", clear_on_submit=True):
                new_p_input = st.text_input("Tên dự án mới:", placeholder="Nhập tên dự án...")
                if st.form_submit_button("Tạo Dự Án Mới", type="primary", use_container_width=True):
                    if new_p_input:
                        conn.execute("INSERT INTO projects (project_name) VALUES (?)", (new_p_input,))
                        conn.commit(); st.rerun()
                    else: st.warning("Chưa nhập tên kìa! 🥺")
            
            st.divider()
            st.markdown("### Xóa dự án")
            if p_opts:
                p_to_del = st.selectbox("Chọn dự án muốn xóa:", list(p_opts.keys()), key="del_proj_select")
                if st.button("❌ Xác nhận Xóa", type="primary", use_container_width=True):
                    if st.session_state.user_info['role'] == 'admin':
                        pid_del = p_opts[p_to_del]
                        conn.execute("DELETE FROM projects WHERE id=?", (pid_del,))
                        conn.execute("DELETE FROM project_links WHERE project_id=?", (pid_del,))
                        conn.commit(); st.rerun()
                    else: st.error("Chỉ Admin mới được xóa thôi nha!")

    if sel_p:
        pid = p_opts[sel_p]
        st.divider()
        st.write(f"Đang liên kết cho: **{sel_p}**")
        
        if not st.session_state.edit_mode:
            if st.button("✏️ Mở Khóa Liên Kết"): st.session_state.edit_mode = True; st.rerun()
        else:
            if st.button("💾 LƯU THAY ĐỔI", type="primary"): st.session_state.trigger_save = True

        all_l = pd.read_sql("SELECT * FROM project_links", conn)
        blocked = all_l[all_l['project_id'] != pid]['invoice_id'].tolist()
        mine = all_l[all_l['project_id'] == pid]['invoice_id'].tolist()
        invs = pd.read_sql("SELECT * FROM invoices WHERE status='active' ORDER BY date DESC", conn)
        avail = invs[~invs['id'].isin(blocked)].copy()
        if not avail.empty:
            avail['Đã chọn'] = avail['id'].isin(mine)
            avail['Tiền'] = avail['total_amount'].apply(format_vnd)
            avail['Tên hóa đơn'] = avail['memo'].fillna('') + " (" + avail['invoice_number'] + ")"
            df_in = avail[avail['type'] == 'IN'][['Đã chọn', 'id', 'Tên hóa đơn', 'Tiền']]
            df_out = avail[avail['type'] == 'OUT'][['Đã chọn', 'id', 'Tên hóa đơn', 'Tiền']]
            dis = ["Tên hóa đơn", "Tiền"]; 
            if not st.session_state.edit_mode: dis.append("Đã chọn")
            cl, cr = st.columns(2)
            with cl:
                st.warning("💸 Hóa đơn Đầu vào") 
                ed_in = st.data_editor(df_in, column_config={"Đã chọn": st.column_config.CheckboxColumn(required=True), "id": None}, disabled=dis, hide_index=True, key="ed_in")
            with cr:
                st.info("💰 Hóa đơn Đầu ra") 
                ed_out = st.data_editor(df_out, column_config={"Đã chọn": st.column_config.CheckboxColumn(required=True), "id": None}, disabled=dis, hide_index=True, key="ed_out")
            if st.session_state.get("trigger_save", False):
                ids = ed_in[ed_in['Đã chọn']]['id'].tolist() + ed_out[ed_out['Đã chọn']]['id'].tolist()
                conn.execute("DELETE FROM project_links WHERE project_id=?", (pid,))
                if ids: conn.executemany("INSERT INTO project_links (project_id, invoice_id) VALUES (?,?)", [(pid, i) for i in ids])
                conn.commit(); st.session_state.edit_mode = False; st.session_state.trigger_save = False; st.rerun()
    conn.close()

# --- TAB 3: BÁO CÁO ---
elif menu == "3. Báo Cáo Tổng Hợp":
    st.title("📊 Báo Cáo Tài Chính")
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    query = '''
        SELECT p.project_name, i.type, i.total_amount, i.date, i.memo
        FROM projects p 
        LEFT JOIN project_links pl ON p.id = pl.project_id
        LEFT JOIN invoices i ON pl.invoice_id = i.id 
        WHERE i.status = 'active' OR i.status IS NULL
    '''
    raw_df = pd.read_sql(query, conn); conn.close()
    if not raw_df.empty:
        raw_df['date_dt'] = pd.to_datetime(raw_df['date'], format='%d/%m/%Y', errors='coerce')
        project_time_map = raw_df[raw_df['type'] == 'OUT'].groupby('project_name')['date_dt'].min().reset_index()
        missing_p = raw_df[~raw_df['project_name'].isin(project_time_map['project_name'])]
        if not missing_p.empty: project_time_map = pd.concat([project_time_map, missing_p.groupby('project_name')['date_dt'].min().reset_index()])
        project_time_map['MonthYear'] = project_time_map['date_dt'].dt.strftime('%m/%Y')
        project_time_map['SortKey'] = project_time_map['date_dt']
        agg_df = raw_df.groupby(['project_name', 'type'])['total_amount'].sum().unstack(fill_value=0).reset_index()
        if 'IN' not in agg_df: agg_df['IN'] = 0
        if 'OUT' not in agg_df: agg_df['OUT'] = 0
        final_report = pd.merge(agg_df, project_time_map[['project_name', 'MonthYear', 'SortKey']], on='project_name')
        final_report['Lãi'] = final_report['OUT'] - final_report['IN']
        final_report = final_report.sort_values(by='SortKey', ascending=False)
        st.metric("TỔNG DOANH THU HỆ THỐNG", format_vnd(final_report['OUT'].sum()))
        st.divider()
        months = final_report['MonthYear'].unique()
        for m in months:
            st.markdown(f"### 📅 Tháng {m}")
            m_data = final_report[final_report['MonthYear'] == m]
            for _, r in m_data.iterrows():
                with st.container():
                    st.markdown(f"""
                    <div class="report-card">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <h4 style="margin:0;">📂 {r['project_name']}</h4>
                            <span class="time-badge">Thời gian: {m}</span>
                        </div>
                        <hr style="margin: 10px 0; border: 0; border-top: 1px solid #eee;">
                        <div style="display: flex; gap: 40px;">
                            <div><small style="opacity:0.8;">Doanh thu:</small><br><b style="font-size:1.2em;">{format_vnd(r['OUT'])}</b></div>
                            <div><small style="opacity:0.8;">Chi phí:</small><br><b style="font-size:1.2em;">{format_vnd(r['IN'])}</b></div>
                            <div style="color: {'#28a745' if r['Lãi'] >= 0 else '#dc3545'};">
                                <small style="opacity:0.8; color:inherit;">Lãi ròng:</small><br><b style="font-size:1.2em; color:inherit;">{format_vnd(r['Lãi'])}</b>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
    else: st.info("Chưa có dữ liệu báo cáo nào hết trơn á 🥺")