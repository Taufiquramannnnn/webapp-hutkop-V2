# app.py
"""
Aplikasi Flask untuk tabel & dashboard koperasi.
- Load file DBF/XLSX dari /uploads, normalisasi, dan agregasi per NOPEG.
- Tersedia filter/pencarian/paginasi + ekspor CSV/Excel/PDF + dashboard ringkasan.
- Filter 'Jenis Pinjaman' berbasis nama file & Bon Cicilan (PDF per karyawan).
- Export BON massal (ZIP) + PDF per orang ikut filter jenis.
- FIX: Notif export via JS.
- BON:
    • Sembunyikan baris dengan Sisa=0.
    • Row TOTAL (hanya “Cicilan” & “Sisa”, muncul jika baris > 1).
    • Tinggi halaman BON auto-fit dengan minimum 265 pt (≈ 9 cm).
- PDF LIST:
    • Tambah baris TOTAL di bawah tabel: Total Karyawan, Total Pinjaman, Total + Bunga, Total Terbayar, Sisa Pinjaman.
"""
# ==============================================================================
# 1) IMPORTS
# ==============================================================================
import os
import io
import glob
import zipfile
import logging
import webbrowser
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, send_file, redirect, url_for, flash
from dbfread import DBF
from custom_parser import CustomFieldParser
import pandas as pd
from werkzeug.utils import secure_filename

# ReportLab (PDF)
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape, A5
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.units import cm

# ==============================================================================
# 2) APP CONFIG
# ==============================================================================
app = Flask(__name__)
app.secret_key = "supersecret"  # Ganti di production

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs("static/css", exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {"dbf", "xlsx"}

# Mapping kolom ke header tampilan
COLUMN_MAPPING = {
    "NOPEG": "No. Pegawai",
    "NAMA": "Nama Karyawan",
    "BAGIAN": "Divisi",
    "JML": "Total Pinjaman",
    "TOTAL_TAGIHAN": "Total + Bunga",
    "LAMA": "Total Tenor",
    "ANGSURAN_KE": "Pembayaran",
    "SISA_ANGSURAN": "Sisa Tenor",
    "DIBAYAR": "Total Terbayar",
    "SISA_CICILAN": "Sisa Pinjaman",
    "STATUS": "Status",
}

# ==============================================================================
# 3) HELPERS
# ==============================================================================
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def classify_loan_type(filename: str) -> str:
    base = os.path.splitext(filename)[0].lower()
    safe = base.replace("_","-").replace(" ","-").replace(".","-").replace("--","-")

    def has_num(n): 
        s=str(n)
        return (f"-{s}" in safe) or (f"{s}-" in safe) or safe.endswith(s) or (f"mot{s}" in safe) or (f"mtr{s}" in safe)

    elek_tokens=("elektronik","elektron","elek","elec","el","elektronil","elektron1")
    motor_tokens=("motor","mot","mtr")
    top_tokens=("topup","top-up","tpup","tu","tup","top")
    uang_tokens=("uang","ua")
    pinj_tokens=("pinjaman","pinjam","pinj","pjm")
    cash_tokens=("cash","tunai","kas","csh")
    dagang_tokens=("dagang","dgg","dgng","hutkopdagang","hutkop1dagang")

    any_in=lambda toks:any(t in safe for t in toks)

    is_elek=any_in(elek_tokens); is_motor=any_in(motor_tokens)
    is_top=any_in(top_tokens); is_uang=any_in(uang_tokens) or ("topupuang" in safe) or ("pinjuang" in safe)
    is_pinj=any_in(pinj_tokens); is_cash=any_in(cash_tokens); is_dagang=any_in(dagang_tokens)

    if is_dagang: return "Hutkop Dagang"
    if is_elek and is_top: return "Elektronik Top Up"
    if is_elek and is_uang: return "Elektronik Uang"
    if is_elek and (has_num(1) or "elek1" in safe or "el1" in safe): return "Elektronik 1"
    if is_motor and has_num(10): return "Motor 10"
    if is_motor and has_num(8): return "Motor 8"
    if is_motor and (has_num(1) or "mot1" in safe or "mtr1" in safe): return "Motor 1"
    if not is_elek and is_top and is_uang: return "Top up uang"
    if is_cash: return "Pinjaman cash"
    if not is_elek and is_pinj and is_uang: return "Pinjaman uang"
    return "Lainnya"

def clean_division_name(name: str) -> str:
    cleaned_name = name.strip().lower()
    if cleaned_name.startswith('penj'):
        return 'Marketing'
    division_map = {
        'adm & k': 'Adm & K', 'adm & keu': 'Adm & K', 'adm & acct': 'Adm & K', 'f & a': 'Adm & K',
        'moa': 'MOA', 'm o a': 'MOA',
        'logistik': 'Logistik', 'logistic': 'Logistik',
        'teknik': 'Teknik', 'tehnik': 'Teknik',
        'busdev': 'Bus-Dev', 'bus-dev': 'Bus-Dev',
        'gdg o. jad': 'Gdg O. Jad', 'g o j': 'Gdg O. Jad',
        'pema-mutu': 'Pem-Mutu', 'pem-mutu': 'Pem-Mutu', 'pemasmutu': 'Pem-Mutu',
        'pros-dev': 'Pros-Dev', 'prosdev': 'Pros-Dev',
        'prod': 'Produksi', 'produksi': 'Produksi',
        'gbb': 'Gdg B. Bak', 'gdg b. bak': 'Gdg B. Bak'
    }
    return division_map.get(cleaned_name, cleaned_name.title())

def read_dbf_file(path: str):
    try:
        table = DBF(path, encoding="latin1", parserclass=CustomFieldParser)
        return [dict(rec) for rec in table]
    except Exception as e:
        logger.error(f"Gagal membaca file DBF {path}: {e}")
        return []

def read_excel_file(path: str):
    try:
        df = pd.read_excel(path, dtype=str)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Gagal membaca file Excel {path}: {e}")
        return []

def _to_float_safe(val, default=0.0):
    if val is None: return default
    s = str(val).strip().replace(" ", "").replace(",", "").replace("\xa0", "")
    if s == "": return default
    try: return float(s)
    except (ValueError, TypeError): return default

def _to_int_safe(val, default=0):
    try:
        f_val = float(str(val).strip())
        return int(f_val) if str(val).strip() != "" else default
    except (ValueError, TypeError, OverflowError):
        return default

def normalize_row(row: dict) -> dict:
    r = dict(row)
    angsuran_terbayar = 0
    for k, v in r.items():
        if str(k).upper().startswith("ANG") and v not in (None, "", b"", 0):
            try:
                if isinstance(v, (int, float)) and v == 0: continue
                if isinstance(v, (datetime, pd.Timestamp)) and pd.isnull(v): continue
                if isinstance(v, str) and not v.strip(): continue
            except Exception:
                pass
            angsuran_terbayar += 1

    r["NOPEG"] = str(r.get("NOPEG") or "").strip()
    r["NAMA"] = str(r.get("NAMA") or "").strip()
    r["BAGIAN"] = clean_division_name(str(r.get("BAGIAN") or ""))

    r["JML"] = _to_float_safe(r.get("JML") or r.get("JML_DDL") or r.get("JUMLAH") or 0, 0.0)
    r["LAMA"] = _to_int_safe(r.get("LAMA") or 0, 0)
    r["CICIL"] = _to_float_safe(r.get("CICIL") or r.get("BUNGA1") or r.get("CICILAN") or 0, 0.0)

    r["ANGSURAN_KE"] = angsuran_terbayar
    r["SISA_ANGSURAN"] = max(r["LAMA"] - angsuran_terbayar, 0)
    r["SISA_CICILAN"] = r["SISA_ANGSURAN"] * r["CICIL"]
    r["TOTAL_TAGIHAN"] = r["LAMA"] * r["CICIL"]
    r["DIBAYAR"] = r["ANGSURAN_KE"] * r["CICIL"]

    if angsuran_terbayar == 0 and r["LAMA"] > 0:
        r["STATUS"] = "Belum Bayar"
    elif r["SISA_ANGSURAN"] <= 0 and r["LAMA"] > 0:
        r["STATUS"] = "Lunas"
    else:
        r["STATUS"] = "Berjalan"
    return r

def load_data():
    try:
        files = glob.glob(os.path.join(UPLOAD_FOLDER, "*.dbf")) + \
                glob.glob(os.path.join(UPLOAD_FOLDER, "*.xlsx"))
        if not files: return []

        all_loans_by_nopeg = {}
        for path in files:
            raw_data = read_dbf_file(path) if path.lower().endswith(".dbf") else read_excel_file(path)
            for rec in raw_data:
                proc = normalize_row(rec)
                filename = os.path.basename(path)
                proc["SRC_FILE"] = filename
                proc["JENIS"] = classify_loan_type(filename)
                nopeg = proc.get("NOPEG")
                if not nopeg: continue
                all_loans_by_nopeg.setdefault(nopeg, []).append(proc)

        final_data = []
        for nopeg, loans in all_loans_by_nopeg.items():
            summary = {
                "JML": sum(l["JML"] for l in loans),
                "LAMA": sum(l["LAMA"] for l in loans),
                "ANGSURAN_KE": sum(l["ANGSURAN_KE"] for l in loans),
                "SISA_ANGSURAN": sum(l["SISA_ANGSURAN"] for l in loans),
                "SISA_CICILAN": sum(l["SISA_CICILAN"] for l in loans),
                "DIBAYAR": sum(l.get("DIBAYAR", 0) for l in loans),
                "TOTAL_TAGIHAN": sum(l.get("TOTAL_TAGIHAN", 0) for l in loans),
            }
            statuses = {l["STATUS"] for l in loans}
            if "Berjalan" in statuses: summary["STATUS"] = "Berjalan"
            elif "Belum Bayar" in statuses: summary["STATUS"] = "Belum Bayar"
            else: summary["STATUS"] = "Lunas"

            final_data.append({
                "NOPEG": nopeg,
                "NAMA": loans[-1]["NAMA"],
                "BAGIAN": loans[-1]["BAGIAN"],
                "SUMMARY": summary,
                "DETAILS": loans,
                "COUNT_PINJAMAN": len(loans),
                "JENIS_SET": sorted({l.get("JENIS", "Lainnya") for l in loans}),
            })
        return final_data
    except Exception as e:
        logger.error(f"Error saat memuat dan memproses data: {str(e)}")
        return []

def _get_filtered_data(search_query: str, bagian_filter: str, status_filter: str, jenis_filter: str) -> list:
    all_data = load_data()
    filtered = all_data
    if search_query:
        s = search_query.lower()
        filtered = [r for r in filtered if s in (r.get("NAMA") or "").lower() or s in (r.get("NOPEG") or "").lower()]
    if bagian_filter:
        b = bagian_filter.lower()
        filtered = [r for r in filtered if (r.get("BAGIAN") or "").lower() == b]
    if status_filter:
        st = status_filter.lower()
        filtered = [r for r in filtered if (r.get("SUMMARY", {}).get("STATUS") or "").lower() == st]
    if jenis_filter:
        filtered = [r for r in filtered if any(d.get("JENIS") == jenis_filter for d in r.get("DETAILS", []))]
    return filtered

# ---------- Helper untuk nama file export (CSV/XLSX/PDF) ----------
def _build_filter_suffix(q: str, bagian: str, status: str, jenis: str) -> str:
    def norm(x: str) -> str:
        return (x or "").strip().replace(" ", "_").replace("/", "-").replace("\\", "-").lower()
    parts = []
    if bagian: parts.append(f"div-{norm(bagian)}")
    if jenis:  parts.append(f"jns-{norm(jenis)}")
    if status: parts.append(f"sts-{norm(status)}")
    if q:      parts.append(f"cari-{norm(q)}")
    return "_".join(parts) if parts else "ALL_DATA"

# ==============================================================================
# 4) ROUTES
# ==============================================================================
@app.route("/", methods=["GET"])
def index():
    try:
        q = request.args.get("search", "").strip()
        bagian_filter = request.args.get("bagian", "").strip()
        status_filter = request.args.get("status", "").strip()
        jenis_filter = request.args.get("jenis", "").strip()
        page = int(request.args.get("page", 1))
        per_page = 20

        filtered_data = _get_filtered_data(q, bagian_filter, status_filter, jenis_filter)

        total_data = len(filtered_data)
        total_pages = (total_data + per_page - 1) // per_page
        start, end = (page - 1) * per_page, (page - 1) * per_page + per_page
        paginated_data = filtered_data[start:end]

        all_raw_data = load_data()
        bagian_list = sorted({r.get("BAGIAN") for r in all_raw_data if r.get("BAGIAN")})
        jenis_list = sorted({d.get("JENIS") for r in all_raw_data for d in r.get("DETAILS", []) if d.get("JENIS")})

        return render_template(
            "index.html",
            data=paginated_data,
            bagian_list=bagian_list,
            jenis_list=jenis_list,
            search=q,
            bagian_selected=bagian_filter,
            status_selected=status_filter,
            jenis_selected=jenis_filter,
            page=page,
            total_pages=total_pages,
            title="Data Koperasi Karyawan",
            column_headers=COLUMN_MAPPING,
            total_filtered=total_data
        )
    except Exception as e:
        logger.error(f"Error di halaman utama: {str(e)}")
        flash("Terjadi kesalahan fatal saat memuat data. Silakan cek log.", "danger")
        return render_template("index.html", data=[], bagian_list=[], jenis_list=[], page=1, total_pages=1)

@app.route("/reset_data", methods=["POST"])
def reset_data():
    try:
        files = glob.glob(os.path.join(UPLOAD_FOLDER, "*"))
        count = 0
        for f in files:
            if f.lower().endswith(tuple(f".{ext}" for ext in ALLOWED_EXTENSIONS)):
                os.remove(f); count += 1
        flash(f"Berhasil mereset data. Sebanyak {count} file data telah dihapus.", "success")
    except Exception as e:
        logger.error(f"Error saat mereset data: {str(e)}")
        flash("Gagal mereset data.", "danger")
    return redirect(url_for("index"))

@app.route("/import", methods=["POST"])
def import_file():
    if "file" not in request.files:
        flash("Tidak ada file yang dipilih untuk di-upload.", "danger")
        return redirect(url_for("index"))

    files = request.files.getlist("file")
    if not files or all(f.filename == "" for f in files):
        flash("Tidak ada file yang dipilih atau nama file kosong.", "danger")
        return redirect(url_for("index"))

    saved_files, errors = [], []
    for file in files:
        if file and file.filename:
            filename = secure_filename(file.filename)
            if not allowed_file(filename):
                errors.append(f"{filename}: Format file tidak didukung (hanya .dbf atau .xlsx).")
                continue
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.exists(filepath):
                base, ext = os.path.splitext(filename)
                ts = int(time.time() * 1000)
                filename = f"{base}_{ts}{ext}"
                filepath = os.path.join(UPLOAD_FOLDER, filename)
            try:
                file.save(filepath)
                saved_files.append(filename)
            except Exception as e:
                logger.error(f"Error saat menyimpan file {filename}: {e}")
                errors.append(f"{filename}: Gagal menyimpan file di server.")

    if saved_files:
        flash(f"Berhasil mengunggah: {', '.join(saved_files)}. Data akan otomatis ditambahkan dan digabungkan.", "success")
    if errors:
        flash("Beberapa file gagal diunggah: " + "; ".join(errors), "warning")
    return redirect(url_for("index"))

# ----------------- EXPORT LIST (CSV / EXCEL / PDF) -----------------
@app.route("/export/csv")
def export_csv():
    try:
        q = request.args.get("search", "").strip()
        bagian = request.args.get("bagian", "").strip()
        status = request.args.get("status", "").strip()
        jenis = request.args.get("jenis", "").strip()
        data = _get_filtered_data(q, bagian, status, jenis)

        if not data:
            flash("Tidak ada data untuk diexpor berdasarkan filter yang dipilih.", "warning")
            return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

        flat_data = []
        for item in data:
            row = {"NOPEG": item["NOPEG"], "NAMA": item["NAMA"], "BAGIAN": item["BAGIAN"]}
            row.update(item["SUMMARY"])
            flat_data.append(row)

        df = pd.DataFrame(flat_data)
        export_keys = [k for k in COLUMN_MAPPING.keys() if k in df.columns]
        float_cols = ['JML', 'TOTAL_TAGIHAN', 'DIBAYAR', 'SISA_CICILAN']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
        df = df[export_keys].rename(columns=COLUMN_MAPPING)

        # === Nama file disesuaikan dengan filter ===
        suffix = _build_filter_suffix(q, bagian, status, jenis)
        filename = f"export_data_koperasi_{suffix}.csv"

        output = io.BytesIO()
        df.to_csv(output, index=False, encoding="utf-8-sig")
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=filename, mimetype='text/csv')
    except Exception as e:
        logger.error(f"Error saat ekspor CSV: {str(e)}")
        flash("Terjadi kesalahan saat mengekspor data ke CSV.", "danger")
        return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

@app.route("/export/excel")
def export_excel():
    try:
        q = request.args.get("search", "").strip()
        bagian = request.args.get("bagian", "").strip()
        status = request.args.get("status", "").strip()
        jenis = request.args.get("jenis", "").strip()
        data = _get_filtered_data(q, bagian, status, jenis)

        if not data:
            flash("Tidak ada data untuk diexpor berdasarkan filter yang dipilih.", "warning")
            return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

        flat_data = []
        for item in data:
            row = {"NOPEG": item["NOPEG"], "NAMA": item["NAMA"], "BAGIAN": item["BAGIAN"]}
            row.update(item["SUMMARY"])
            flat_data.append(row)

        df = pd.DataFrame(flat_data)
        export_keys = [k for k in COLUMN_MAPPING.keys() if k in df.columns]
        float_cols = ['JML', 'TOTAL_TAGIHAN', 'DIBAYAR', 'SISA_CICILAN']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(float)
        df = df[export_keys].rename(columns=COLUMN_MAPPING)

        # === Nama file disesuaikan dengan filter ===
        suffix = _build_filter_suffix(q, bagian, status, jenis)
        filename = f"export_data_koperasi_{suffix}.xlsx"

        output = io.BytesIO()
        df.to_excel(output, index=False, sheet_name='Data Koperasi')
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=filename,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        logger.error(f"Error saat ekspor Excel: {str(e)}")
        flash("Terjadi kesalahan saat mengekspor data ke Excel.", "danger")
        return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

@app.route("/export/pdf")
def export_pdf():
    try:
        q = request.args.get("search", "").strip()
        bagian = request.args.get("bagian", "").strip()
        status = request.args.get("status", "").strip()
        jenis = request.args.get("jenis", "").strip()
        data = _get_filtered_data(q, bagian, status, jenis)

        if not data:
            flash("Tidak ada data untuk diexpor berdasarkan filter yang dipilih.", "warning")
            return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

        # === Nama file disesuaikan dengan filter ===
        suffix = _build_filter_suffix(q, bagian, status, jenis)
        filename = f"export_data_koperasi_{suffix}.pdf"

        pdf_buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer, pagesize=landscape(A4),
            rightMargin=1*cm, leftMargin=1*cm, topMargin=1*cm, bottomMargin=1*cm
        )

        styles = getSampleStyleSheet()
        style_title = ParagraphStyle(name='Title', parent=styles['h1'], alignment=TA_CENTER, spaceAfter=6, fontSize=14)
        style_subtitle = ParagraphStyle(name='Subtitle', parent=styles['Normal'], alignment=TA_CENTER, spaceAfter=12, fontSize=9, textColor=colors.grey)
        style_body_left = ParagraphStyle(name='BodyLeft', parent=styles['Normal'], alignment=TA_LEFT, fontSize=7, leading=9)
        style_body_center = ParagraphStyle(name='BodyCenter', parent=styles['Normal'], alignment=TA_CENTER, fontSize=7, leading=9)
        style_header = ParagraphStyle(name='Header', parent=styles['Normal'], alignment=TA_CENTER, fontName='Helvetica-Bold', fontSize=8, textColor=colors.black)
        style_body_left_b = ParagraphStyle(name='BodyLeftBold', parent=style_body_left, fontName='Helvetica-Bold')
        style_body_center_b = ParagraphStyle(name='BodyCenterBold', parent=style_body_center, fontName='Helvetica-Bold')

        elements = [Paragraph("Data Koperasi Karyawan", style_title)]

        filter_texts = []
        if bagian: filter_texts.append(f"Divisi: {bagian}")
        if jenis: filter_texts.append(f"Jenis: {jenis}")
        if status: filter_texts.append(f"Status: {status}")
        if q: filter_texts.append(f"Cari: '{q}'")
        subtitle_text = "(Filter Aktif: " + " | ".join(filter_texts) + ")" if filter_texts else "(Semua Data)"
        elements.append(Paragraph(subtitle_text, style_subtitle))

        header = [Paragraph(text, style_header) for text in COLUMN_MAPPING.values()]
        table_data = [header]
        keys = list(COLUMN_MAPPING.keys())
        for item in data:
            s = item["SUMMARY"]
            row_dict = {
                "NOPEG": item.get("NOPEG",""), "NAMA": item.get("NAMA",""), "BAGIAN": item.get("BAGIAN",""),
                "JML": s.get("JML", 0), "TOTAL_TAGIHAN": s.get("TOTAL_TAGIHAN", 0), "LAMA": s.get("LAMA", 0),
                "ANGSURAN_KE": s.get("ANGSURAN_KE", 0), "SISA_ANGSURAN": s.get("SISA_ANGSURAN", 0),
                "DIBAYAR": s.get("DIBAYAR", 0), "SISA_CICILAN": s.get("SISA_CICILAN", 0), "STATUS": s.get("STATUS", "")
            }
            row_cells = []
            for k in keys:
                v = row_dict.get(k, "")
                if k in ("JML", "TOTAL_TAGIHAN", "DIBAYAR", "SISA_CICILAN"):
                    v = f"{float(v):,.0f}".replace(',', '.')
                style = style_body_center if k not in ("NAMA","BAGIAN") else style_body_left
                row_cells.append(Paragraph(str(v), style))
            table_data.append(row_cells)

        # === BARIS TOTAL ===
        total_karyawan = len(data)
        total_jml = sum(r["SUMMARY"].get("JML", 0) for r in data)
        total_tagihan = sum(r["SUMMARY"].get("TOTAL_TAGIHAN", 0) for r in data)
        total_dibayar = sum(r["SUMMARY"].get("DIBAYAR", 0) for r in data)
        total_sisa = sum(r["SUMMARY"].get("SISA_CICILAN", 0) for r in data)

        total_row = [""] * len(keys)
        total_row[0] = Paragraph(f"TOTAL KARYAWAN: {total_karyawan}", style_body_left_b)

        idx_jml = keys.index("JML")
        idx_tot = keys.index("TOTAL_TAGIHAN")
        idx_dby = keys.index("DIBAYAR")
        idx_sisa = keys.index("SISA_CICILAN")

        total_row[idx_jml] = Paragraph(f"Rp {total_jml:,.0f}".replace(',', '.'), style_body_center_b)
        total_row[idx_tot] = Paragraph(f"Rp {total_tagihan:,.0f}".replace(',', '.'), style_body_center_b)
        total_row[idx_dby] = Paragraph(f"Rp {total_dibayar:,.0f}".replace(',', '.'), style_body_center_b)
        total_row[idx_sisa] = Paragraph(f"Rp {total_sisa:,.0f}".replace(',', '.'), style_body_center_b)

        table_data.append(total_row)

        col_widths = [2.2*cm, 4.3*cm, 2.5*cm, 2.8*cm, 2.8*cm, 2.0*cm, 2.2*cm, 2.0*cm, 2.8*cm, 2.8*cm, 2.0*cm]
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        tbl_style = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef1f4")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ]

        last_row = len(table_data) - 1
        tbl_style += [
            ("BACKGROUND", (0, last_row), (-1, last_row), colors.HexColor("#f5f7fa")),
            ("FONTNAME", (0, last_row), (-1, last_row), "Helvetica-Bold"),
            ("SPAN", (0, last_row), (2, last_row)),  # merge "No Pegawai".."Divisi"
        ]

        table.setStyle(TableStyle(tbl_style))
        elements.append(table)
        doc.build(elements)

        pdf_buffer.seek(0)
        return send_file(pdf_buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')

    except Exception as e:
        logger.error(f"Error saat ekspor PDF: {str(e)}")
        flash("Terjadi kesalahan saat mengekspor data ke PDF.", "danger")
        return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

# ----------------- EXPORT BON (PER ORANG & MASSAL) -----------------
BON_PAGE_SIZE = A5
BON_MARGIN_LEFT = 1*cm
BON_MARGIN_RIGHT = 1*cm
BON_MARGIN_TOP = .8*cm
BON_MARGIN_BOTTOM = .8*cm

# Tinggi BON dinamis (unit points)
def _compute_bon_pagesize(num_rows: int) -> tuple:
    width = BON_PAGE_SIZE[0]
    banner_h = 20
    spacer = 6
    id_row_h = 14
    header_row_h = 16
    row_h = 16
    footer_h = 24

    id_block = id_row_h * 2
    table_h = header_row_h + max(num_rows, 1) * row_h
    content_height = banner_h + spacer + id_block + spacer + table_h + spacer + footer_h
    margin_height = BON_MARGIN_TOP + BON_MARGIN_BOTTOM
    total_height = content_height + margin_height
    return (width, max(total_height, 265))  # min 265pt

def build_bon_story(person: dict, jenis_filter: str | None = None, page_width: float | None = None):
    styles = getSampleStyleSheet()
    small = ParagraphStyle(name='Small', parent=styles['Normal'], fontSize=8, leading=10)
    small_b = ParagraphStyle(name='SmallB', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold')
    now = datetime.now().strftime('%d-%m-%Y %H:%M')
    page_w = page_width or BON_PAGE_SIZE[0]
    available_width = page_w - BON_MARGIN_LEFT - BON_MARGIN_RIGHT

    story = []
    banner = Table(
        [[Paragraph("BON CICILAN", ParagraphStyle(name='Banner', alignment=TA_LEFT, textColor=colors.black, fontSize=11, fontName='Helvetica-Bold'))]],
        colWidths=[available_width]
    )
    banner.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#eef1f4")),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
        ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("ALIGN", (0,0), (-1,-1), "LEFT")
    ]))
    story.extend([banner, Spacer(0, 6)])

    id_weights = [2.0, 6.2, 1.8, 2.8]
    id_cols = [available_width * (w / sum(id_weights)) for w in id_weights]
    header_data = [
        [Paragraph("Nama", small_b), Paragraph(person.get("NAMA", "-"), small),
         Paragraph("Nomor", small_b), Paragraph(person.get("NOPEG", "-"), small)],
        [Paragraph("Bagian", small_b), Paragraph(person.get("BAGIAN", "-"), small),
         Paragraph("Dicetak", small_b), Paragraph(now, small)]
    ]
    t1 = Table(header_data, colWidths=id_cols)
    t1.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP"), ("BOTTOMPADDING", (0,0), (-1,-1), 4)]))
    story.extend([t1, Spacer(0, 4)])

    detail_weights = [4.5, 1.8, 1.3, 2.6, 2.6]
    detail_cols = [available_width * (w / sum(detail_weights)) for w in detail_weights]
    rows = [[Paragraph("Jenis", small_b), Paragraph("Cicilan ke", small_b), Paragraph("Tenor", small_b),
             Paragraph("Cicilan", small_b), Paragraph("Sisa", small_b)]]

    details = person.get("DETAILS", [])

    def _to_num(x):
        try: return float(x)
        except: return 0.0

    details_filtered = [
        d for d in details
        if (not jenis_filter or d.get("JENIS") == jenis_filter)
        and _to_num(d.get("SISA_CICILAN", 0)) > 0
    ]

    sum_cicilan = 0.0
    sum_sisa = 0.0
    if not details_filtered:
        rows.append([Paragraph("Tidak ada pinjaman berjalan (sisa = 0 tidak ditampilkan).", small),
                     "", "", "", ""])
        nrows_for_height = 1
    else:
        for d in details_filtered:
            cicil = _to_num(d.get('CICIL', 0))
            sisa = _to_num(d.get('SISA_CICILAN', 0))
            sum_cicilan += cicil
            sum_sisa += sisa
            rows.append([
                Paragraph(d.get("JENIS", "-"), small),
                Paragraph(str(int(_to_num(d.get("ANGSURAN_KE", 0)))), small),
                Paragraph(str(int(_to_num(d.get("LAMA", 0)))), small),
                Paragraph(f"Rp {int(cicil):,}".replace(',', '.'), small),
                Paragraph(f"Rp {int(sisa):,}".replace(',', '.'), small)
            ])

        show_total = len(details_filtered) > 1
        if show_total:
            rows.append([
                Paragraph("TOTAL", small_b),
                Paragraph("", small_b),
                Paragraph("", small_b),
                Paragraph(f"Rp {int(sum_cicilan):,}".replace(',', '.'), small_b),
                Paragraph(f"Rp {int(sum_sisa):,}".replace(',', '.'), small_b),
            ])
        nrows_for_height = len(details_filtered) + (1 if show_total else 0)

    t2 = Table(rows, colWidths=detail_cols, repeatRows=1)
    cmds = [
        ("GRID", (0,0), (-1,-1), 0.3, colors.grey),
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor('#f3f4f6')),
        ("TEXTCOLOR", (0,0), (-1,0), colors.black),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ("ALIGN", (0,0), (0,-1), "LEFT"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]
    if details_filtered and len(details_filtered) > 1:
        last = len(rows) - 1
        cmds += [("BACKGROUND", (0,last), (-1,last), colors.HexColor("#eef1f4")),
                 ("FONTNAME", (0,last), (-1,last), "Helvetica-Bold")]
    if not details_filtered:
        cmds += [('SPAN', (0,1), (-1,1)), ('ALIGN', (0,1), (-1,1), 'CENTER')]

    t2.setStyle(TableStyle(cmds))
    story.append(t2)

    story.extend([
        Spacer(0, 8),
        Paragraph("Unit Simpan Pinjam KOPKAROPI", small_b),
        Paragraph("Bon ini dicetak dari sistem. Harap simpan sebagai arsip.", small)
    ])
    return story, nrows_for_height

def render_bon_pdf_to_bytes(person: dict, jenis_filter: str | None = None) -> bytes:
    _, nrows = build_bon_story(person, jenis_filter=jenis_filter, page_width=BON_PAGE_SIZE[0])
    page_size = _compute_bon_pagesize(nrows)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=page_size,
                            rightMargin=BON_MARGIN_RIGHT, leftMargin=BON_MARGIN_LEFT,
                            topMargin=BON_MARGIN_TOP, bottomMargin=BON_MARGIN_BOTTOM)
    story, _ = build_bon_story(person, jenis_filter=jenis_filter, page_width=page_size[0])
    doc.build(story)
    return buf.getvalue()

@app.route("/export/bon/<nopeg>")
def export_bon(nopeg):
    try:
        jenis_filter = request.args.get("jenis", "").strip() or None
        data = load_data()
        person = next((x for x in data if x.get("NOPEG") == nopeg), None)
        if not person:
            flash("Data karyawan tidak ditemukan.", "warning")
            q = request.args.get("search", "")
            bagian = request.args.get("bagian", "")
            status = request.args.get("status", "")
            return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis_filter))

        filename = f"bon_{nopeg}_{person.get('NAMA', 'noname').replace(' ','_')}.pdf"
        pdf_buffer = io.BytesIO()

        _, nrows = build_bon_story(person, jenis_filter=jenis_filter, page_width=BON_PAGE_SIZE[0])
        page_size = _compute_bon_pagesize(nrows)

        doc = SimpleDocTemplate(pdf_buffer, pagesize=page_size,
                                rightMargin=BON_MARGIN_RIGHT, leftMargin=BON_MARGIN_LEFT,
                                topMargin=BON_MARGIN_TOP, bottomMargin=BON_MARGIN_BOTTOM)
        story, _ = build_bon_story(person, jenis_filter=jenis_filter, page_width=page_size[0])
        doc.build(story)

        pdf_buffer.seek(0)
        return send_file(pdf_buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')
    except Exception as e:
        logger.error(f"Error saat ekspor bon {nopeg}: {str(e)}")
        q = request.args.get("search", "")
        bagian = request.args.get("bagian", "")
        status = request.args.get("status", "")
        jenis = request.args.get("jenis", "")
        flash(f"Terjadi kesalahan saat mencetak bon untuk {nopeg}.", "danger")
        return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

@app.route("/export/bon_bulk")
def export_bon_bulk():
    try:
        q = request.args.get("search", "").strip()
        bagian = request.args.get("bagian", "").strip()
        status = request.args.get("status", "").strip()
        jenis = request.args.get("jenis", "").strip()

        filtered_data = _get_filtered_data(q, bagian, status, jenis)
        if not filtered_data:
            flash("Tidak ada data untuk diexpor berdasarkan filter yang dipilih.", "warning")
            return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            filter_parts = []
            if bagian: filter_parts.append(f"div-{bagian.replace(' ','_').lower()}")
            if jenis: filter_parts.append(f"jns-{jenis.replace(' ','_').lower()}")
            if status: filter_parts.append(f"sts-{status.replace(' ','_').lower()}")
            if q: filter_parts.append(f"cari-{q.replace(' ','_').lower()}")
            subfolder = "_".join(filter_parts) if filter_parts else "ALL_DATA"

            for person in filtered_data:
                try:
                    pdf_bytes = render_bon_pdf_to_bytes(person, jenis_filter=jenis or None)
                    nopeg = person.get("NOPEG", "UNKNOWN")
                    nama = (person.get("NAMA", "NONAME") or "").replace(" ", "_").replace("/", "-").replace("\\","-").replace(":","-").replace("*","-").replace("?","-").replace("\"","-").replace("<","-").replace(">","-").replace("|","-")
                    zf.writestr(f"{subfolder}/bon_{nopeg}_{nama}.pdf", pdf_bytes)
                except Exception as person_err:
                    logger.error(f"Gagal membuat bon untuk {person.get('NOPEG', 'N/A')} di ZIP: {person_err}")
                    try:
                        zf.writestr(f"{subfolder}/ERROR_{person.get('NOPEG', 'N_A')}.txt", f"Gagal membuat PDF Bon: {str(person_err)}")
                    except Exception as zip_err:
                        logger.error(f"Gagal menulis file error ke ZIP: {zip_err}")

        zip_buf.seek(0)
        dl_name = f"bon_koperasi_{subfolder}.zip"
        return send_file(zip_buf, as_attachment=True, download_name=dl_name, mimetype="application/zip")

    except Exception as e:
        logger.error(f"Error saat ekspor bon massal: {str(e)}")
        flash("Terjadi kesalahan saat membuat ZIP Bon.", "danger")
        q = request.args.get("search", "")
        bagian = request.args.get("bagian", "")
        status = request.args.get("status", "")
        jenis = request.args.get("jenis", "")
        return redirect(url_for('index', search=q, bagian=bagian, status=status, jenis=jenis))

# ----------------- DASHBOARD (tanpa perubahan) -----------------
@app.route("/dashboard")
def dashboard():
    try:
        all_data = load_data()
        if not all_data:
            return render_template(
                "dashboard.html", title="Dashboard Ringkasan",
                total_pinjaman_pokok=0, total_tagihan=0, sisa_pinjaman=0, total_karyawan=0,
                status_details={"labels": [], "counts": [], "amounts": [], "percentages": []},
                bagian_count={}, top_borrowers=[],
                bagian_pinjaman={}, bagian_sisa={}, bagian_dibayar={}
            )
        total_karyawan = len(all_data)
        total_pinjaman_pokok = sum(r["SUMMARY"].get("JML", 0) for r in all_data)
        total_tagihan_semua = sum(r["SUMMARY"].get("TOTAL_TAGIHAN", 0) for r in all_data)
        total_sisa_semua = sum(r["SUMMARY"].get("SISA_CICILAN", 0) for r in all_data)

        status_labels = ["Lunas", "Berjalan", "Belum Bayar"]
        status_count = {k: 0 for k in status_labels}
        status_amount = {k: 0 for k in status_labels}
        for r in all_data:
            st = r["SUMMARY"].get("STATUS", "Berjalan")
            status_count[st] = status_count.get(st, 0) + 1
            if st != "Lunas":
                status_amount[st] = status_amount.get(st, 0) + r["SUMMARY"].get("SISA_CICILAN", 0)

        status_details = {
            "labels": status_labels,
            "counts": [status_count.get(k, 0) for k in status_labels],
            "amounts": [status_amount.get(k, 0) for k in status_labels],
            "percentages": [round((status_count.get(k, 0) / total_karyawan) * 100, 1) if total_karyawan else 0 for k in status_labels]
        }

        bagian_count_raw, bagian_total_raw, bagian_sisa_raw = {}, {}, {}
        for r in all_data:
            bagian = r.get("BAGIAN") or "Tidak Ada Divisi"
            bagian_count_raw[bagian] = bagian_count_raw.get(bagian, 0) + 1
            total_kontrak = r["SUMMARY"].get("TOTAL_TAGIHAN", 0)
            sisa = r["SUMMARY"].get("SISA_CICILAN", 0)
            bagian_total_raw[bagian] = bagian_total_raw.get(bagian, 0) + total_kontrak
            bagian_sisa_raw[bagian] = bagian_sisa_raw.get(bagian, 0) + sisa

        bagian_dibayar_raw = {k: max(bagian_total_raw.get(k, 0) - bagian_sisa_raw.get(k, 0), 0) for k in bagian_total_raw.keys()}
        sorted_bagian_total = sorted(bagian_total_raw.items(), key=lambda x: x[1], reverse=True)[:10]
        top_10_bagian_pinjaman = {k: v for k, v in sorted_bagian_total}
        top_10_bagian_sisa = {k: bagian_sisa_raw.get(k, 0) for k, _ in sorted_bagian_total}
        top_10_bagian_dibayar = {k: bagian_dibayar_raw.get(k, 0) for k, _ in sorted_bagian_total}
        sorted_bagian_count = sorted(bagian_count_raw.items(), key=lambda x: x[1], reverse=True)[:10]
        top_10_bagian_count = {k: v for k, v in sorted_bagian_count}

        sorted_by_kontrak = sorted(all_data, key=lambda x: x["SUMMARY"].get("TOTAL_TAGIHAN", 0), reverse=True)
        top_borrowers = []
        for r in sorted_by_kontrak[:10]:
            total_kontrak = r["SUMMARY"].get("TOTAL_TAGIHAN", 0)
            sisa = r["SUMMARY"].get("SISA_CICILAN", 0)
            dibayar = max(total_kontrak - sisa, 0)
            top_borrowers.append({
                "nama": r.get("NAMA", "N/A"),
                "jumlah": total_kontrak,
                "sisa": sisa,
                "dibayar": dibayar
            })

        return render_template(
            "dashboard.html",
            title="Dashboard Ringkasan",
            status_details=status_details,
            bagian_count=top_10_bagian_count,
            bagian_pinjaman=top_10_bagian_pinjaman,
            bagian_sisa=top_10_bagian_sisa,
            bagian_dibayar=top_10_bagian_dibayar,
            total_pinjaman_pokok=total_pinjaman_pokok,
            total_tagihan=total_tagihan_semua,
            sisa_pinjaman=total_sisa_semua,
            total_karyawan=total_karyawan,
            top_borrowers=top_borrowers,
        )
    except Exception as e:
        logger.error(f"Error di halaman dashboard: {str(e)}")
        flash("Terjadi kesalahan saat memuat data dashboard.", "danger")
        return redirect(url_for("index"))

# ==============================================================================
# 5) ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    def open_browser():
        webbrowser.open("http://127.0.0.1:5000/")
    threading.Timer(1.0, open_browser).start()
    app.run(debug=False)
