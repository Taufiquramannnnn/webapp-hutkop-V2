# Aplikasi Dashboard Pinjaman Koperasi Karyawan

Aplikasi web sederhana berbasis **Flask** dan **Bootstrap 5** untuk mengelola, menampilkan, dan menganalisis data pinjaman koperasi karyawan. Dibuat untuk mempermudah agregasi data dari banyak file `.dbf` atau `.xlsx` menjadi satu tampilan terpusat.

## Fitur Utama

-   **Agregasi Data Otomatis**: Menggabungkan beberapa file `.dbf` dan `.xlsx` menjadi satu ringkasan data per karyawan.
-   **Tampilan Tabel Interaktif**: Dilengkapi fitur pencarian, filter berdasarkan divisi & status, serta paginasi.
-   **Detail Pinjaman**: Lihat rincian setiap pinjaman per karyawan dengan mudah.
-   **Dashboard Visual**: Ringkasan data dalam bentuk chart untuk total pinjaman, status, dan peminjam terbesar per divisi.
-   **Export Data**: Ekspor data ringkasan ke format **CSV**, **Excel**, dan **PDF**.
-   **Manajemen Data**: Fitur untuk upload file baru dan reset (hapus semua) data.

## Teknologi yang Digunakan

-   **Backend**: Flask (Python)
-   **Frontend**: HTML, CSS, JavaScript, Bootstrap 5
-   **Data Processing**: Pandas, DBFRead
-   **PDF Generation**: ReportLab
-   **Charting**: Chart.js

## Cara Menjalankan Aplikasi

1.  **Clone Repository**
    ```bash
    git clone [https://github.com/Taufiquramannnnn/webapp-hutkop-V2.git](https://github.com/Taufiquramannnnn/webapp-hutkop-V2.git)
    cd webapp-hutkop-V2
    ```

2.  **Setup Virtual Environment**
    ```bash
    # Buat environment baru
    python -m venv venv

    # Aktifkan environment (Windows)
    .\venv\Scripts\activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Jalankan Aplikasi**
    ```bash
    python app.py
    ```
    Aplikasi akan otomatis terbuka di browser pada alamat `http://127.0.0.1:5000/`.