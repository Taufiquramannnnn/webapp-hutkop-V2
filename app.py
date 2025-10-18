"""
app.py
------
Aplikasi Flask untuk tabel & dashboard koperasi.
- Load file DBF/XLSX dari /uploads, normalisasi, dan agregasi per NOPEG.
- Tersedia filter/pencarian/paginasi + ekspor CSV/Excel/PDF + dashboard ringkasan.
"""

# ==============================================================================
# 1) IMPORTS
# ==============================================================================
import os
import glob
import logging
import webbrowser
import threading
import time
from flask import Flask, render_template, request, send_file, redirect, url_for, flash
from dbfread import DBF
from custom_parser import CustomFieldParser
import pandas as pd
from werkzeug.utils import secure_filename

# ReportLab (PDF)
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.units import cm

# ==============================================================================
# 2) APP CONFIG
# ==============================================================================
app = Flask(__name__)
app.secret_key = "supersecret"

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs("static/css", exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {"dbf", "xlsx"}

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
    """Cek ekstensi file, cuma izinkan .dbf atau .xlsx."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def clean_division_name(name: str) -> str:
    """Normalisasi nama divisi yang berantakan."""
    name = name.strip().lower()
    
    # Kamus untuk standardisasi nama divisi
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
    
    # Cari di kamus, kalau tidak ada, kembalikan nama asli dengan format Title Case
    return division_map.get(name, name.title())


def read_dbf_file(path: str):
    """Baca file DBF. Pakai parser custom & fallback ke list kosong jika error."""
    try:
        table = DBF(path, encoding="latin1", parserclass=CustomFieldParser)
        return [dict(rec) for rec in table]
    except Exception as e:
        logger.error(f"Gagal membaca file DBF {path}: {e}")
        return []

def read_excel_file(path: str):
    """Baca file Excel. Fallback ke list kosong jika error."""
    try:
        df = pd.read_excel(path, dtype=str)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Gagal membaca file Excel {path}: {e}")
        return []

def _to_float_safe(val, default=0.0):
    """Konversi nilai ke float, aman dari string kosong atau format aneh."""
    if val is None:
        return default
    s = str(val).strip().replace(" ", "").replace(",", "").replace("\xa0", "")
    if s == "":
        return default
    try:
        return float(s)
    except:
        return default

def _to_int_safe(val, default=0):
    """Konversi nilai ke integer, aman dari string kosong atau format aneh."""
    try:
        return int(float(str(val).strip())) if str(val).strip() != "" else default
    except:
        return default

def normalize_row(row: dict) -> dict:
    """
    Normalisasi satu baris data pinjaman.
    - Membersihkan & konversi tipe data.
    - Menghitung field turunan seperti sisa cicilan, status, dll.
    """
    r = dict(row)

    # Hitung angsuran yang sudah dibayar dari kolom 'ANG...'.
    # Anggap setiap kolom 'ANG...' yang tidak kosong/nol adalah satu kali bayar.
    angsuran_terbayar = 0
    for k, v in r.items():
        if str(k).upper().startswith("ANG") and v not in (None, "", b"", 0):
            try:
                if isinstance(v, (int, float)) and v == 0:
                    continue
            except:
                pass
            angsuran_terbayar += 1

    # Normalisasi field dasar (teks)
    r["NOPEG"]  = str(r.get("NOPEG") or "").strip()
    r["NAMA"]   = str(r.get("NAMA") or "").strip()
    # Gunakan fungsi cleaning untuk divisi
    r["BAGIAN"] = clean_division_name(str(r.get("BAGIAN") or ""))

    # Normalisasi field angka
    r["JML"]   = _to_float_safe(r.get("JML") or r.get("JML_DDL") or r.get("JUMLAH") or 0, 0.0)
    r["LAMA"]  = _to_int_safe(r.get("LAMA") or 0, 0)
    r["CICIL"] = _to_float_safe(r.get("CICIL") or r.get("BUNGA1") or r.get("CICILAN") or 0, 0.0)

    # Hitung field turunan
    r["ANGSURAN_KE"]   = angsuran_terbayar
    r["SISA_ANGSURAN"] = max(r["LAMA"] - angsuran_terbayar, 0)
    r["SISA_CICILAN"]  = r["SISA_ANGSURAN"] * r["CICIL"]
    r["TOTAL_TAGIHAN"] = r["LAMA"] * r["CICIL"]
    r["DIBAYAR"]       = r["ANGSURAN_KE"] * r["CICIL"]

    # Tentukan status pinjaman
    if angsuran_terbayar == 0 and r["LAMA"] > 0:
        r["STATUS"] = "Belum Bayar"
    elif r["SISA_ANGSURAN"] <= 0 and r["LAMA"] > 0:
        r["STATUS"] = "Lunas"
    else:
        r["STATUS"] = "Berjalan"

    return r

def load_data():
    """
    Core function:
    1. Scan semua file .dbf & .xlsx di folder /uploads.
    2. Baca & normalisasi setiap baris dari semua file.
    3. Kelompokkan semua pinjaman berdasarkan NOPEG.
    4. Agregasi (jumlahkan) semua pinjaman untuk setiap NOPEG.
    5. Return list data karyawan yang sudah diagregasi.
    """
    try:
        files = glob.glob(os.path.join(UPLOAD_FOLDER, "*.dbf")) + \
                glob.glob(os.path.join(UPLOAD_FOLDER, "*.xlsx"))

        if not files:
            return []

        # Step 1-3: Baca semua file dan kelompokkan per NOPEG
        all_loans_by_nopeg = {}
        for path in files:
            raw_data = read_dbf_file(path) if path.lower().endswith(".dbf") else read_excel_file(path)
            for rec in raw_data:
                proc = normalize_row(rec)
                proc["SRC_FILE"] = os.path.basename(path) # Simpan nama file asal
                nopeg = proc.get("NOPEG")
                if not nopeg:
                    continue
                all_loans_by_nopeg.setdefault(nopeg, []).append(proc)

        # Step 4: Agregasi data
        final_data = []
        for nopeg, loans in all_loans_by_nopeg.items():
            # Jumlahkan semua metrik pinjaman untuk satu orang
            summary = {
                "JML": sum(l["JML"] for l in loans),
                "LAMA": sum(l["LAMA"] for l in loans),
                "ANGSURAN_KE": sum(l["ANGSURAN_KE"] for l in loans),
                "SISA_ANGSURAN": sum(l["SISA_ANGSURAN"] for l in loans),
                "SISA_CICILAN": sum(l["SISA_CICILAN"] for l in loans),
                "DIBAYAR": sum(l.get("DIBAYAR", 0) for l in loans),
                "TOTAL_TAGIHAN": sum(l.get("TOTAL_TAGIHAN", 0) for l in loans),
            }

            # Tentukan status agregat: jika ada satu saja pinjaman 'Berjalan', statusnya 'Berjalan'.
            statuses = {l["STATUS"] for l in loans}
            if "Berjalan" in statuses:
                summary["STATUS"] = "Berjalan"
            elif "Belum Bayar" in statuses:
                summary["STATUS"] = "Belum Bayar"
            else:
                summary["STATUS"] = "Lunas"

            final_data.append({
                "NOPEG": nopeg,
                "NAMA": loans[-1]["NAMA"], # Ambil nama & bagian dari data terakhir
                "BAGIAN": loans[-1]["BAGIAN"],
                "SUMMARY": summary,       # Hasil agregasi
                "DETAILS": loans,         # List pinjaman individual
                "COUNT_PINJAMAN": len(loans),
            })

        return final_data

    except Exception as e:
        logger.error(f"Error saat memuat dan memproses data: {str(e)}")
        return []

# ==============================================================================
# 4) ROUTES
# ==============================================================================

@app.route("/", methods=["GET"])
def index():
    """Route untuk halaman utama (tabel data karyawan)."""
    try:
        all_data = load_data()

        # Ambil parameter filter dari URL
        q = request.args.get("search", "").strip().lower()
        bagian_filter = request.args.get("bagian", "").strip()
        status_filter = request.args.get("status", "").strip()
        page = int(request.args.get("page", 1))
        per_page = 20

        # Apply filter
        filtered = all_data
        if q:
            filtered = [r for r in filtered if q in (r.get("NAMA") or "").lower() or q in (r.get("NOPEG") or "").lower()]
        if bagian_filter:
            filtered = [r for r in filtered if (r.get("BAGIAN") or "").lower() == bagian_filter.lower()]
        if status_filter:
            filtered = [r for r in filtered if (r.get("SUMMARY", {}).get("STATUS") or "").lower() == status_filter.lower()]

        # Paginasi
        total_data = len(filtered)
        total_pages = (total_data + per_page - 1) // per_page
        start = (page - 1) * per_page
        end = start + per_page
        paginated_data = filtered[start:end]

        # Siapkan list divisi untuk dropdown filter (setelah di-clean)
        bagian_list = sorted({r.get("BAGIAN") for r in all_data if r.get("BAGIAN")})

        return render_template(
            "index.html", data=paginated_data, bagian_list=bagian_list,
            search=q, bagian_selected=bagian_filter, status_selected=status_filter,
            page=page, total_pages=total_pages, title="Data Koperasi Karyawan",
            column_headers=COLUMN_MAPPING, total_filtered=total_data
        )
    except Exception as e:
        logger.error(f"Error di halaman utama: {str(e)}")
        flash("Terjadi kesalahan fatal saat memuat data. Silakan cek log.", "danger")
        return render_template("index.html", data=[], bagian_list=[], page=1, total_pages=1)


@app.route("/reset_data", methods=["POST"])
def reset_data():
    """Endpoint untuk hapus semua file data di folder /uploads."""
    try:
        files = glob.glob(os.path.join(UPLOAD_FOLDER, "*"))
        count = 0
        for f in files:
            if f.lower().endswith((".dbf", ".xlsx")):
                os.remove(f); count += 1
        flash(f"Berhasil mereset data. Sebanyak {count} file data telah dihapus.", "success")
    except Exception as e:
        logger.error(f"Error saat mereset data: {str(e)}")
        flash("Gagal mereset data.", "danger")
    return redirect(url_for("index"))

@app.route("/import", methods=["POST"])
def import_file():
    """Endpoint untuk handle upload file data."""
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
                ts = int(time.time())
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

# --- Export Routes ---
@app.route("/export/csv")
def export_csv():
    """Export data ke format CSV."""
    try:
        data = load_data()
        flat_data = []
        for item in data:
            row = {"NOPEG": item["NOPEG"], "NAMA": item["NAMA"], "BAGIAN": item["BAGIAN"]}
            row.update(item["SUMMARY"])
            flat_data.append(row)

        df = pd.DataFrame(flat_data)

        float_cols = ['JML', 'TOTAL_TAGIHAN', 'DIBAYAR', 'SISA_CICILAN']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        df = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)

        filename = "export_data_koperasi.csv"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        return send_file(filepath, as_attachment=True, download_name=filename)
    except Exception as e:
        logger.error(f"Error saat ekspor CSV: {str(e)}")
        flash("Terjadi kesalahan saat mengekspor data ke CSV.", "danger")
        return redirect(url_for("index"))

@app.route("/export/excel")
def export_excel():
    """Export data ke format Excel."""
    try:
        data = load_data()
        flat_data = []
        for item in data:
            row = {"NOPEG": item["NOPEG"], "NAMA": item["NAMA"], "BAGIAN": item["BAGIAN"]}
            row.update(item["SUMMARY"])
            flat_data.append(row)

        df = pd.DataFrame(flat_data)
        df = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)

        filename = "export_data_koperasi.xlsx"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        df.to_excel(filepath, index=False)
        return send_file(filepath, as_attachment=True, download_name=filename)
    except Exception as e:
        logger.error(f"Error saat ekspor Excel: {str(e)}")
        flash("Terjadi kesalahan saat mengekspor data ke Excel.", "danger")
        return redirect(url_for("index"))

@app.route("/export/pdf")
def export_pdf():
    """Export data ke format PDF (landscape)."""
    try:
        data = load_data()
        filename = "export_data_koperasi.pdf"
        filepath = os.path.join(UPLOAD_FOLDER, filename)

        doc = SimpleDocTemplate(
            filepath, pagesize=landscape(A4),
            rightMargin=1*cm, leftMargin=1*cm, topMargin=1*cm, bottomMargin=1*cm
        )

        styles = getSampleStyleSheet()
        style_title = ParagraphStyle(name='Title', parent=styles['h1'], alignment=TA_CENTER, spaceAfter=12, fontSize=14)
        style_body_left = ParagraphStyle(name='BodyLeft', parent=styles['Normal'], alignment=TA_LEFT, fontSize=7, leading=9)
        style_body_center = ParagraphStyle(name='BodyCenter', parent=styles['Normal'], alignment=TA_CENTER, fontSize=7, leading=9)
        style_header = ParagraphStyle(name='Header', parent=styles['Normal'], alignment=TA_CENTER, fontName='Helvetica-Bold', fontSize=8, textColor=colors.whitesmoke)

        elements = [Paragraph("Data Koperasi Karyawan", style_title)]
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
                    v = f"{float(v):,.0f}"
                row_cells.append(Paragraph(str(v), style_body_center if k not in ("NAMA","BAGIAN") else style_body_left))
            table_data.append(row_cells)

        col_widths = [2.2*cm, 4.3*cm, 2.8*cm, 2.5*cm, 2.5*cm, 2.3*cm, 2.3*cm, 2.3*cm, 2.5*cm, 2.5*cm, 2.0*cm]

        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#343a40")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ]))

        elements.append(table)
        doc.build(elements)
        return send_file(filepath, as_attachment=True, download_name=filename)

    except Exception as e:
        logger.error(f"Error saat ekspor PDF: {str(e)}")
        flash("Terjadi kesalahan saat mengekspor data ke PDF.", "danger")
        return redirect(url_for("index"))

@app.route("/dashboard")
def dashboard():
    """Route untuk halaman dashboard ringkasan."""
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

        # 1. Agregasi untuk KPI & Chart Status
        status_count = {"Lunas": 0, "Berjalan": 0, "Belum Bayar": 0}
        status_amount = {"Lunas": 0, "Berjalan": 0, "Belum Bayar": 0}
        for r in all_data:
            status = r["SUMMARY"]["STATUS"]
            if status in status_count:
                status_count[status] += 1
            if status != "Lunas":
                status_amount[status] += r["SUMMARY"]["SISA_CICILAN"]

        total_karyawan = len(all_data)
        status_details = {
            "labels": list(status_count.keys()),
            "counts": list(status_count.values()),
            "amounts": list(status_amount.values()),
            "percentages": [round((count / total_karyawan) * 100, 1) if total_karyawan > 0 else 0 for count in status_count.values()]
        }

        # 2. Agregasi untuk Chart Divisi
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

        # 3. Kalkulasi KPI utama
        total_pinjaman_pokok = sum(r["SUMMARY"].get("JML", 0) for r in all_data)
        total_tagihan_semua = sum(r["SUMMARY"].get("TOTAL_TAGIHAN", 0) for r in all_data)
        total_sisa_semua = sum(r["SUMMARY"].get("SISA_CICILAN", 0) for r in all_data)


        # 4. Data untuk top 10 peminjam terbesar
        sorted_by_kontrak = sorted(all_data, key=lambda x: x["SUMMARY"].get("TOTAL_TAGIHAN", 0), reverse=True)
        top_borrowers = []
        for r in sorted_by_kontrak[:10]:
            # FIX: Gunakan .get() untuk akses aman, mencegah error jika key tidak ada
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