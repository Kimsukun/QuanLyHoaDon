import streamlit as st
import pandas as pd
import pdfplumber
import re
from datetime import datetime
import time
import base64
import hashlib
import sqlite3
import os
import shutil
from PIL import Image
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

# ==========================================
# 1. CẤU HÌNH TRANG & KHỞI TẠO MÔI TRƯỜNG
# ==========================================
st.set_page_config(page_title="Quản Lý Hóa Đơn Pro (Local)", page_icon="📑", layout="wide")

# --- QUẢN LÝ SESSION STATE ---
if "logged_in" not in st.session_state: st.session_state.logged_in = False
if "user_info" not in st.session_state: st.session_state.user_info = None
if "db_initialized" not in st.session_state: st.session_state.db_initialized = False

# FIX LỖI OUT TÀI KHOẢN: Dùng thư mục ẩn
UPLOAD_FOLDER = ".uploaded_invoices"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

DB_FILE = "invoice_app.db"

# ==========================================
# 2. XỬ LÝ DATABASE (SQLite)
# ==========================================
def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# Hàm cập nhật cấu trúc bảng nếu là DB cũ (Migration)
def migrate_db_columns():
    conn = get_connection()
    c = conn.cursor()
    try:
        # Thêm cột drive_link nếu chưa có
        c.execute("ALTER TABLE invoices ADD COLUMN drive_link TEXT")
    except: pass
    
    try:
        # Thêm cột request_edit (0: ko, 1: có yêu cầu duyệt sửa)
        c.execute("ALTER TABLE invoices ADD COLUMN request_edit INTEGER DEFAULT 0")
    except: pass
    conn.commit()
    conn.close()

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password TEXT, role TEXT, status TEXT)''')
    # Thêm sẵn drive_link và request_edit vào bảng invoices
    c.execute('''CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, date TEXT, invoice_number TEXT, invoice_symbol TEXT, 
        seller_name TEXT, buyer_name TEXT, pre_tax_amount REAL, tax_amount REAL, total_amount REAL, 
        file_name TEXT, status TEXT, edit_count INTEGER, created_at TEXT, memo TEXT, file_path TEXT,
        drive_link TEXT, request_edit INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, project_name TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS project_links (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, invoice_id INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS company_info (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, address TEXT, phone TEXT, logo_base64 TEXT)''')

    # Data mặc định
    c.execute("SELECT * FROM users WHERE username = 'admin'")
    if not c.fetchone():
        admin_pw = hashlib.sha256("admin123".encode()).hexdigest()
        c.execute("INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)", ('admin', admin_pw, 'admin', 'approved'))
    
    c.execute("SELECT * FROM company_info WHERE id = 1")
    if not c.fetchone():
        c.execute("INSERT INTO company_info (name, address, phone, logo_base64) VALUES (?, ?, ?, ?)", ('Tên Công Ty Của Bạn', 'Địa chỉ...', '090...', ''))

    conn.commit()
    conn.close()

if not st.session_state.db_initialized:
    init_db()
    migrate_db_columns() # Chạy migration để update cột mới
    st.session_state.db_initialized = True

# --- CÁC HÀM HỖ TRỢ ---
def run_query(query, params=(), fetch_one=False, commit=False):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute(query, params)
        if commit:
            conn.commit()
            return True
        if fetch_one:
            return c.fetchone()
        return c.fetchall()
    except Exception as e:
        return None
    finally:
        conn.close()

def hash_pass(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def save_file_local(uploaded_file, is_converted_pdf=False, pdf_bytes=None):
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if is_converted_pdf:
            # Nếu là file PDF được convert từ ảnh
            final_name = f"{ts}_converted_image.pdf"
            file_path = os.path.join(UPLOAD_FOLDER, final_name)
            with open(file_path, "wb") as f:
                f.write(pdf_bytes)
        else:
            # File gốc
            clean_name = re.sub(r'[\\/*?:"<>|]', "", uploaded_file.name)
            final_name = f"{ts}_{clean_name}"
            file_path = os.path.join(UPLOAD_FOLDER, final_name)
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
                
        return file_path, final_name
    except: return None, None

def format_vnd(amount):
    if amount is None: return "0"
    try: return "{:,.0f}".format(float(amount)).replace(",", ".")
    except: return "0"

def get_company_data():
    row = run_query("SELECT * FROM company_info WHERE id = 1", fetch_one=True)
    if row:
        return {'name': row['name'], 'address': row['address'], 'phone': row['phone'], 'logo_b64_str': row['logo_base64']}
    return {'name': 'Company', 'address': '...', 'phone': '...', 'logo_b64_str': ''}

def update_company_info(name, address, phone, logo_bytes=None):
    b64_str = base64.b64encode(logo_bytes).decode('utf-8') if logo_bytes else ""
    if not logo_bytes:
        old = run_query("SELECT logo_base64 FROM company_info WHERE id = 1", fetch_one=True)
        if old: b64_str = old['logo_base64']
    run_query("UPDATE company_info SET name=?, address=?, phone=?, logo_base64=? WHERE id=1", (name, address, phone, b64_str), commit=True)
    st.cache_data.clear()

# ==========================================
# 3. CSS & XỬ LÝ FILE (PDF/IMAGE -> PDF)
# ==========================================
comp = get_company_data()
st.markdown("""
<style>
    .stApp { background-color: var(--background-color); font-family: 'Segoe UI', sans-serif; }
    .money-box { 
        background: linear-gradient(135deg, #1e7e34 0%, #28a745 100%) !important;
        color: #ffffff !important; padding: 20px; border-radius: 12px; 
        box-shadow: 0 4px 15px rgba(40, 167, 69, 0.4); font-size: 1.2em; font-weight: bold; text-align: center;
    }
    .report-card, .login-container { 
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 12px; padding: 20px; margin-bottom: 15px; 
        transition: transform 0.2s; 
    }
    .report-card:hover { transform: translateY(-3px); border-color: #28a745; }
    .company-header { display: flex; align-items: center; justify-content: center; gap: 25px; padding: 20px; border-bottom: 1px solid #ddd; margin-bottom: 20px; }
    .header-logo { border-radius: 10px; object-fit: contain; }
    .stButton button { border-radius: 8px; font-weight: 600; text-transform: uppercase; }
    
    /* Style cho hàng bị xóa */
    .deleted-row { background-color: #ffe6e6; padding: 10px; border-radius: 5px; margin-bottom: 5px; border: 1px solid #ffcccc; opacity: 0.8; }
    .active-row { background-color: transparent; padding: 10px; border-bottom: 1px solid #eee; margin-bottom: 5px; }
</style>
""", unsafe_allow_html=True)

def extract_numbers_from_line(line):
    raw_nums = re.findall(r'(?<!\d)(?!0\d)\d{1,3}(?:[.,]\d{3})+(?![.,]\d)', line)
    return [float(n.replace('.', '').replace(',', '')) for n in raw_nums if not (1990 <= float(n.replace('.', '').replace(',', '')) <= 2030)]

# --- HÀM CHUYỂN ẢNH SANG PDF ---
def convert_image_to_pdf(image_file):
    try:
        img = Image.open(image_file)
        # Chuyển sang RGB nếu cần
        if img.mode != 'RGB':
            img = img.convert('RGB')
            
        img_width, img_height = img.size
        
        # Tạo PDF buffer
        pdf_buffer = io.BytesIO()
        c = canvas.Canvas(pdf_buffer, pagesize=(img_width, img_height))
        
        # Lưu ảnh tạm thời để vẽ vào PDF (reportlab cần đường dẫn file ảnh)
        temp_img_path = f"temp_img_{int(time.time())}.jpg"
        img.save(temp_img_path)
        
        c.drawImage(temp_img_path, 0, 0, img_width, img_height)
        c.save()
        
        # Xóa ảnh tạm
        if os.path.exists(temp_img_path):
            os.remove(temp_img_path)
            
        pdf_buffer.seek(0)
        return pdf_buffer
    except Exception as e:
        return None

def extract_data_smart(file_obj, is_image=False):
    text_content = ""
    msg = None
    
    try:
        # Nếu là ảnh, convert sang PDF trước
        pdf_file = file_obj
        if is_image:
            pdf_buffer = convert_image_to_pdf(file_obj)
            if pdf_buffer:
                pdf_file = pdf_buffer
            else:
                return None, "Lỗi chuyển đổi ảnh sang PDF"

        # Dùng pdfplumber để đọc (hoạt động tốt với cả PDF gốc và PDF từ ảnh nếu ảnh rõ nét)
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages: 
                extracted = page.extract_text()
                if extracted:
                    text_content += extracted + "\n"
        
        # Nếu PDF (từ ảnh) mà không trích xuất được text -> Cần OCR (Tesseract)
        # Ở đây ta giả định pdfplumber đọc được text cơ bản. Nếu không, trả về thông báo nhập tay.
        if not text_content.strip():
             return {"date": "", "seller": "", "buyer": "", "inv_num": "", "inv_sym": "", "pre_tax": 0.0, "tax": 0.0, "total": 0.0, "all_numbers": []}, "Không đọc được chữ từ file này. Vui lòng nhập tay."

    except Exception as e: return None, f"Lỗi đọc file: {str(e)}"
    
    all_found_numbers = set()
    info = {"date": "", "seller": "", "buyer": "", "inv_num": "", "inv_sym": "", "pre_tax": 0.0, "tax": 0.0, "total": 0.0, "all_numbers": []}
    
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
        
    if info["total"] == 0 and all_found_numbers: info["total"] = max(all_found_numbers)
    if info["pre_tax"] == 0: info["pre_tax"] = round(info["total"] / 1.08)
    if info["tax"] == 0: info["tax"] = info["total"] - info["pre_tax"]
    
    for line in lines[:35]:
        l_c = line.strip()
        if re.search(r'^(Đơn vị bán|Người bán|Bên A|Nhà cung cấp)', l_c, re.IGNORECASE): info["seller"] = l_c.split(':')[-1].strip()
        elif re.search(r'^(Đơn vị mua|Người mua|Khách hàng|Bên B)', l_c, re.IGNORECASE): info["buyer"] = l_c.split(':')[-1].strip()
        
    info["all_numbers"] = list(all_found_numbers) 
    return info, msg

# ==========================================
# 4. GIAO DIỆN CHÍNH
# ==========================================
if not st.session_state.logged_in:
    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_b:
        st.write("")
        if comp['logo_b64_str']:
            st.markdown(f'<div style="text-align:center;"><img src="data:image/png;base64,{comp["logo_b64_str"]}" height="120" class="header-logo"></div>', unsafe_allow_html=True)
        st.markdown(f"""<div style="text-align:center; margin-top:20px;"><h1 style="color:#28a745 !important;">{comp['name']}</h1><p>📍 {comp['address']}<br>📞 {comp['phone']}</p></div>""", unsafe_allow_html=True)
        
        tab_login, tab_reg = st.tabs(["🔐 Đăng nhập", "📝 Đăng ký"])
        with tab_login:
            with st.form("login"):
                u = st.text_input("Tài khoản"); p = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("ĐĂNG NHẬP", use_container_width=True):
                    user = run_query("SELECT * FROM users WHERE username=? AND password=?", (u, hash_pass(p)), fetch_one=True)
                    if user and user['status'] == 'approved':
                        st.session_state.logged_in = True
                        st.session_state.user_info = {"name": user['username'], "role": user['role']}
                        st.rerun()
                    else: st.error("Sai thông tin hoặc chưa được duyệt!")
        with tab_reg:
            with st.form("reg"):
                nu = st.text_input("Tài khoản mới"); np = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("ĐĂNG KÝ", use_container_width=True):
                    try:
                        run_query("INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)", (nu, hash_pass(np), 'user', 'pending'), commit=True)
                        st.success("Đã gửi yêu cầu!")
                    except: st.error("Tên đã tồn tại!")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    if comp['logo_b64_str']: st.markdown(f'<div style="text-align:center;"><img src="data:image/png;base64,{comp["logo_b64_str"]}" width="150" style="border-radius:10px;"></div>', unsafe_allow_html=True)
    st.success(f"Chào, **{st.session_state.user_info['name']}**")
    
    if st.session_state.user_info['role'] == 'admin':
        with st.expander("⚙️ Admin Panel"):
            st.caption("1. Duyệt User")
            for u in run_query("SELECT * FROM users WHERE role='user'") or []:
                c1, c2, c3 = st.columns([2,1,1])
                c1.write(f"{u['username']} ({u['status']})")
                if u['status'] == 'pending' and c2.button("✔", key=f"a_{u['id']}"):
                    run_query("UPDATE users SET status='approved' WHERE id=?", (u['id'],), commit=True); st.rerun()
                if c3.button("✖", key=f"d_{u['id']}"):
                    run_query("DELETE FROM users WHERE id=?", (u['id'],), commit=True); st.rerun()
            
            st.divider(); st.caption("2. Duyệt Yêu Cầu Sửa Giá")
            # --- ADMIN DUYỆT YÊU CẦU SỬA ---
            req_invoices = run_query("SELECT * FROM invoices WHERE request_edit=1 AND status='active'")
            if req_invoices:
                for r in req_invoices:
                    with st.container():
                        st.info(f"HĐ: {r['invoice_number']} | Tiền: {format_vnd(r['total_amount'])}")
                        ca, cb = st.columns(2)
                        if ca.button("Duyệt (Reset)", key=f"app_e_{r['id']}"):
                            # Reset count và bỏ cờ request
                            run_query("UPDATE invoices SET edit_count=0, request_edit=0 WHERE id=?", (r['id'],), commit=True)
                            st.success("Đã duyệt!"); time.sleep(0.5); st.rerun()
                        if cb.button("Từ chối", key=f"den_e_{r['id']}"):
                            run_query("UPDATE invoices SET request_edit=0 WHERE id=?", (r['id'],), commit=True)
                            st.rerun()
            else:
                st.caption("Không có yêu cầu nào.")

            st.divider(); st.caption("3. Cập nhật thông tin")
            with st.form("comp_update"):
                cn = st.text_input("Tên", value=comp['name'])
                ca = st.text_input("Địa chỉ", value=comp['address'])
                cp = st.text_input("SĐT", value=comp['phone'])
                ul = st.file_uploader("Logo", type=['png','jpg'])
                if st.form_submit_button("Lưu"):
                    update_company_info(cn, ca, cp, ul.read() if ul else None)
                    st.success("Xong!"); time.sleep(0.5); st.rerun()
            
            st.divider()
            if st.button("🗑️ Xóa TẤT CẢ hóa đơn", type="primary"):
                run_query("DELETE FROM invoices", commit=True)
                run_query("DELETE FROM sqlite_sequence WHERE name='invoices'", commit=True)
                if os.path.exists(UPLOAD_FOLDER):
                    for f in os.listdir(UPLOAD_FOLDER):
                        try: os.remove(os.path.join(UPLOAD_FOLDER, f))
                        except: pass
                st.toast("Đã xóa sạch!"); time.sleep(1); st.rerun()

    if st.button("Đăng xuất", use_container_width=True):
        st.session_state.logged_in = False; st.rerun()
    st.divider()
    menu = st.radio("MENU", ["1. Nhập Hóa Đơn", "2. Liên Kết Dự Án", "3. Báo Cáo Tổng Hợp"])

# --- HEADER ---
l_html = f'<img src="data:image/png;base64,{comp["logo_b64_str"]}" height="80" class="header-logo">' if comp['logo_b64_str'] else ''
st.markdown(f'<div class="company-header">{l_html}<div><h1 style="margin:0; color:#28a745;">{comp["name"]}</h1><p style="margin:0;">{comp["address"]} | {comp["phone"]}</p></div></div>', unsafe_allow_html=True)

# State init
if "pdf_data" not in st.session_state: st.session_state.pdf_data = None
if "edit_lock" not in st.session_state: st.session_state.edit_lock = True
if "local_edit_count" not in st.session_state: st.session_state.local_edit_count = 0
if "uploader_key" not in st.session_state: st.session_state.uploader_key = 0
if "uploaded_file_obj" not in st.session_state: st.session_state.uploaded_file_obj = None

# --- TAB 1: NHẬP HÓA ĐƠN ---
if menu == "1. Nhập Hóa Đơn":
    uploaded_file = st.file_uploader("Upload Hóa Đơn (PDF/Ảnh)", type=["pdf", "png", "jpg", "jpeg"], key=f"up_{st.session_state.uploader_key}")
    show_pdf = st.checkbox("Xem File", value=True)
    
    if uploaded_file:
        # Lưu file tạm vào session state để dùng khi lưu
        st.session_state.uploaded_file_obj = uploaded_file
        
        c_pdf, c_form = st.columns([1,1]) if show_pdf else (None, st.container())
        if show_pdf:
            with c_pdf:
                try:
                    # Hiển thị PDF
                    if "pdf" in uploaded_file.type:
                        with pdfplumber.open(uploaded_file) as pdf:
                            st.info(f"{len(pdf.pages)} trang")
                            for i, p in enumerate(pdf.pages):
                                st.image(p.to_image(resolution=300).original, caption=f"Trang {i+1}", use_container_width=True)
                    # Hiển thị Ảnh
                    else:
                        st.image(uploaded_file, caption="Ảnh hóa đơn", use_container_width=True)
                except: st.error("Lỗi hiển thị file")
        
        with c_form:
            if st.button("🔍 PHÂN TÍCH", type="primary", use_container_width=True):
                is_img = "pdf" not in uploaded_file.type
                data, msg = extract_data_smart(uploaded_file, is_image=is_img)
                
                if msg: st.warning(msg)
                
                data['file_name'] = uploaded_file.name
                st.session_state.pdf_data = data; st.session_state.edit_lock = True; st.session_state.local_edit_count = 0
                diff = abs(data['total'] - (data['pre_tax'] + data['tax']))
                if diff < 10: st.success(f"✅ Chuẩn! Tổng: {format_vnd(data['total'])}")
                else: st.warning(f"⚠️ Lệch: {format_vnd(diff)}")

            if st.session_state.pdf_data:
                d = st.session_state.pdf_data
                with st.form("inv_form"):
                    typ = st.radio("Loại", ["Đầu vào", "Đầu ra"], horizontal=True)
                    # Thêm Link Drive
                    drive_link = st.text_input("🔗 Link Drive (Tùy chọn)")
                    
                    memo = st.text_input("Gợi nhớ", value=d.get('file_name',''))
                    date = st.text_input("Ngày", value=d['date'])
                    c1, c2 = st.columns(2)
                    num = c1.text_input("Số", value=d['inv_num']); sym = c2.text_input("Ký hiệu", value=d['inv_sym'])
                    st.divider()
                    seller = st.text_input("Bên Bán", value=d['seller'])
                    buyer = st.text_input("Bên Mua", value=d['buyer'])
                    
                    st.markdown("#### 💰 Tiền")
                    pre = st.number_input("Tiền hàng", value=float(d['pre_tax']), disabled=st.session_state.edit_lock, format="%.0f")
                    tax = st.number_input("VAT", value=float(d['tax']), disabled=st.session_state.edit_lock, format="%.0f")
                    total = pre + tax
                    
                    # CẢNH BÁO CHỈNH SỬA & LOGIC ADMIN
                    is_locked_admin = False
                    if st.session_state.local_edit_count == 1:
                        st.markdown('<div style="background:#ffeef7; color:red; padding:10px; border-radius:5px; margin-bottom:10px;">🌸 <b>Lần sửa 1/2:</b> Cẩn thận nha!</div>', unsafe_allow_html=True)
                    elif st.session_state.local_edit_count >= 2:
                        is_locked_admin = True
                        st.markdown('<div style="background:#fff3cd; color:orange; padding:10px; border-radius:5px; margin-bottom:10px;">🍊 <b>Hết lượt sửa!</b> Cần gửi yêu cầu Admin duyệt để lưu.</div>', unsafe_allow_html=True)

                    st.markdown(f'<div class="money-box">{format_vnd(total)}</div>', unsafe_allow_html=True)
                    
                    b1, b2 = st.columns(2)
                    if b1.form_submit_button("✏️ Sửa giá"):
                        if not is_locked_admin:
                            st.session_state.edit_lock = False; st.rerun()
                        else: st.error("Đã hết lượt sửa!")
                    
                    if not st.session_state.edit_lock and b2.form_submit_button("✅ Chốt giá"):
                        st.session_state.pdf_data.update({'pre_tax': pre, 'tax': tax, 'total': total})
                        st.session_state.edit_lock = True; st.session_state.local_edit_count += 1; st.rerun()

                    # NÚT LƯU THAY ĐỔI THEO TRẠNG THÁI
                    btn_label = "🚀 GỬI YÊU CẦU DUYỆT" if is_locked_admin else "💾 LƯU HÓA ĐƠN"
                    
                    if st.form_submit_button(btn_label, type="primary", use_container_width=True):
                        if not date or not num: st.error("Thiếu ngày/số!")
                        elif not st.session_state.edit_lock: st.warning("Chốt giá trước!")
                        else:
                            # Xử lý lưu file (Nếu là ảnh thì convert sang PDF để lưu)
                            f_obj = st.session_state.uploaded_file_obj
                            f_obj.seek(0)
                            
                            is_img = "pdf" not in f_obj.type
                            pdf_bytes = None
                            if is_img:
                                pdf_buffer = convert_image_to_pdf(f_obj)
                                if pdf_buffer: pdf_bytes = pdf_buffer.getvalue()
                            
                            if is_img and pdf_bytes:
                                # Lưu PDF đã convert
                                path, final_name = save_file_local(f_obj, is_converted_pdf=True, pdf_bytes=pdf_bytes)
                            else:
                                # Lưu file gốc (PDF)
                                path, final_name = save_file_local(f_obj)

                            if path:
                                t = 'OUT' if "Đầu ra" in typ else 'IN'
                                req_flag = 1 if is_locked_admin else 0
                                
                                run_query("""INSERT INTO invoices 
                                (type, date, invoice_number, invoice_symbol, seller_name, buyer_name, 
                                pre_tax_amount, tax_amount, total_amount, file_name, status, 
                                edit_count, created_at, memo, file_path, drive_link, request_edit) 
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                          (t, date, num, sym, seller, buyer, pre, tax, total, final_name, 
                                           'active', st.session_state.local_edit_count, 
                                           datetime.now().strftime("%Y-%m-%d %H:%M:%S"), memo, path, drive_link, req_flag), commit=True)
                                
                                if is_locked_admin: st.success("Đã gửi yêu cầu duyệt cho Admin!")
                                else: st.success("Đã lưu thành công!")
                                
                                time.sleep(1)
                                st.session_state.pdf_data = None; st.session_state.uploader_key += 1; st.session_state.uploaded_file_obj = None; st.rerun()

    st.divider()
    with st.expander("Lịch sử", expanded=True):
        # Lấy tất cả (kể cả xóa) để hiển thị
        rows = run_query("SELECT * FROM invoices ORDER BY id DESC LIMIT 15")
        if rows:
            df = pd.DataFrame([dict(r) for r in rows])
            df['Tiền'] = df['total_amount'].apply(format_vnd)
            
            for _, r in df.iterrows():
                # Xử lý giao diện hàng xóa
                bg_style = "deleted-row" if r['status'] == 'deleted' else "active-row"
                req_msg = " | ⏳ Đang chờ duyệt sửa" if r.get('request_edit') == 1 else ""
                
                with st.container():
                    st.markdown(f"""
                        <div class="{bg_style}" style="display: flex; align-items: center; justify-content: space-between;">
                            <div style="flex:1"><b>#{r['id']}</b></div>
                            <div style="flex:1">{r['type']}</div>
                            <div style="flex:3">{r['memo']} | {r['invoice_number']} {req_msg}</div>
                            <div style="flex:2; font-weight:bold;">{r['Tiền']}</div>
                            <div style="flex:1">{r['status']}</div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    # Nút chức năng (chỉ hiện cho active)
                    if r['status'] == 'active' and st.session_state.user_info['role'] == 'admin':
                        if st.button("❌ Hủy", key=f"del_{r['id']}"):
                            run_query("UPDATE invoices SET status='deleted' WHERE id=?", (r['id'],), commit=True); st.rerun()

# --- TAB 2: LIÊN KẾT DỰ ÁN ---
elif menu == "2. Liên Kết Dự Án":
    c1, c2 = st.columns([2,1])
    projs = run_query("SELECT * FROM projects")
    p_map = {r['project_name']: r['id'] for r in projs} if projs else {}
    
    with c1: selected_p = st.selectbox("Dự Án:", list(p_map.keys()) if p_map else [], key="sp")
    with c2:
        with st.popover("➕/🗑️ Dự án"):
            with st.form("new_p"):
                if st.form_submit_button("Tạo") and (n := st.text_input("Tên")):
                    run_query("INSERT INTO projects (project_name, created_at) VALUES (?,?)", (n, datetime.now().strftime("%Y-%m-%d")), commit=True); st.rerun()
            if p_map:
                d_p = st.selectbox("Xóa", list(p_map.keys()))
                if st.button("Xóa") and st.session_state.user_info['role'] == 'admin':
                    run_query("DELETE FROM projects WHERE id=?", (p_map[d_p],), commit=True)
                    run_query("DELETE FROM project_links WHERE project_id=?", (p_map[d_p],), commit=True); st.rerun()

    if selected_p:
        pid = p_map[selected_p]
        if "edit_mode" not in st.session_state: st.session_state.edit_mode = False
        
        # LOGIC ẨN HÓA ĐƠN ĐÃ THUỘC DỰ ÁN KHÁC
        all_links = run_query("SELECT * FROM project_links")
        blocked_ids = {l['invoice_id'] for l in all_links if l['project_id'] != pid}
        current_ids = {l['invoice_id'] for l in all_links if l['project_id'] == pid}

        c_btn, _ = st.columns([1,5])
        if not st.session_state.edit_mode:
            if c_btn.button("✏️ Chỉnh sửa"): st.session_state.edit_mode = True; st.rerun()
        else:
            if c_btn.button("💾 LƯU"): st.session_state.trigger_save = True

        all_inv = run_query("SELECT * FROM invoices WHERE status='active' ORDER BY date DESC")
        if all_inv:
            df = pd.DataFrame([dict(r) for r in all_inv])
            df = df[~df['id'].isin(blocked_ids)]
            
            if not df.empty:
                df['Selected'] = df['id'].isin(current_ids)
                df['Show'] = df['memo'].fillna('') + " (" + df['total_amount'].apply(format_vnd) + ")"
                
                c_in, c_out = st.columns(2)
                dis = not st.session_state.edit_mode
                
                with c_in:
                    st.warning("Đầu vào")
                    d_in = df[df['type']=='IN'][['Selected','id','Show']]
                    e_in = st.data_editor(d_in, column_config={"Selected": st.column_config.CheckboxColumn(required=True), "id": None}, disabled=dis, hide_index=True, key="e_in")
                with c_out:
                    st.info("Đầu ra")
                    d_out = df[df['type']=='OUT'][['Selected','id','Show']]
                    e_out = st.data_editor(d_out, column_config={"Selected": st.column_config.CheckboxColumn(required=True), "id": None}, disabled=dis, hide_index=True, key="e_out")

                if st.session_state.get("trigger_save"):
                    s_ids = []
                    if not e_in.empty: s_ids += e_in[e_in['Selected']]['id'].tolist()
                    if not e_out.empty: s_ids += e_out[e_out['Selected']]['id'].tolist()
                    
                    run_query("DELETE FROM project_links WHERE project_id=?", (pid,), commit=True)
                    for i in s_ids:
                        run_query("INSERT INTO project_links (project_id, invoice_id) VALUES (?,?)", (pid, i), commit=True)
                    
                    st.session_state.edit_mode = False; st.session_state.trigger_save = False; st.success("Đã lưu!"); st.rerun()
            else: st.info("Không còn hóa đơn trống.")

# --- TAB 3: BÁO CÁO ---
elif menu == "3. Báo Cáo Tổng Hợp":
    st.title("📊 Báo Cáo Tài Chính")
    
    # --- BỘ LỌC THÁNG ---
    all_dates = run_query("SELECT date FROM invoices WHERE status='active'")
    valid_dates = []
    for r in all_dates:
        try: valid_dates.append(datetime.strptime(r['date'], "%d/%m/%Y"))
        except: pass
    
    months = sorted(list(set([d.strftime("%m/%Y") for d in valid_dates])), reverse=True)
    selected_month = st.selectbox("📅 Chọn Tháng Lọc Dự Án (Bỏ trống = Tất cả)", ["Tất cả"] + months)

    base_query = """
        SELECT p.project_name, i.type, i.total_amount, i.date
        FROM projects p
        JOIN project_links l ON p.id = l.project_id
        JOIN invoices i ON l.invoice_id = i.id
        WHERE i.status = 'active'
    """
    rows = run_query(base_query)
    
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        if selected_month != "Tất cả":
            df['dt'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')
            df['MyMonth'] = df['dt'].dt.strftime('%m/%Y')
            df = df[df['MyMonth'] == selected_month]

        if not df.empty:
            agg = df.groupby(['project_name', 'type'])['total_amount'].sum().unstack(fill_value=0).reset_index()
            if 'IN' not in agg: agg['IN'] = 0
            if 'OUT' not in agg: agg['OUT'] = 0
            agg['Lãi'] = agg['OUT'] - agg['IN']
            
            st.metric(f"LỢI NHUẬN TỔNG ({selected_month})", format_vnd(agg['Lãi'].sum()))
            
            for _, r in agg.iterrows():
                with st.container():
                    st.markdown(f"""
                    <div class="report-card">
                        <h4>📂 {r['project_name']}</h4><hr style="margin:5px 0;">
                        <div style="display:flex; justify-content:space-between;">
                            <span>Thu: <b>{format_vnd(r['OUT'])}</b></span>
                            <span>Chi: <b>{format_vnd(r['IN'])}</b></span>
                            <span style="color:{'#28a745' if r['Lãi']>=0 else 'red'}">Lãi: <b>{format_vnd(r['Lãi'])}</b></span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        else: st.info(f"Không có dữ liệu cho tháng {selected_month}")
    else: st.info("Chưa có dữ liệu.")
