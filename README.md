<div align="center">

### Grocery Orders Analytics Dashboard - Pipeline Detailing Explanation

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-latest-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![Metabase](https://img.shields.io/badge/Metabase-latest-509EE3?style=flat-square&logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

</div>

---

## 📋 Table of Contents

1. [Kelompok 1](#-team)
2. [Pipeline Architecture](#️-pipeline-architecture)
3. [Fetch API & Data Ingestion](#-fetch-api--data-ingestion)
4. [Dataset Explanation & EDA](#-dataset-explanation--eda)
5. [Spark Processing](#-spark-processing)
6. [Docker Setup](#-docker-setup)
7. [Proof: ClickHouse & Airflow](#-proof--clickhouse--airflow)
8. [Data Visualization](#-data-visualization)
9. [Dashboard Overview](#-dashboard-overview)
10. [Insights](#-insights)
11. [End-to-End Running Guide](#-end-to-end-running-guide)
12. [Closing](#-closing)

---

## 👥 Kelompok 1

<table align="center">
    <!-- <p align="center">
    <img src="asset_documentation/farel-ferel.gif" width="700"/>
    </p> -->
    <td align="center" width="320">
      <b>Ibrahim Ferel</b>
      <br>
      <code>5025241049</code>
      <br><br>
    </td>
    <td align="center" width="320">
      <b>Afarrel Febryan Putra Andy</b>
      <br>
      <code>5025241137</code>
      <br><br>
    </td>
  </tr>
</table>

---

## 🗺️ Pipeline Architecture

Pipeline ini mengikuti arsitektur **ELT (Extract -> Load -> Transform)** berbasis Big Data dengan orkestrasi penuh menggunakan Apache Airflow.

![alt text](asset_documentation/image.png)

### Flow Summary

| Step | Script | Output |
|------|--------|--------|
| Fetch | `fetch_task2_stream.py` | `.parquet` files di Data Lake |
| Process | `process_task2_spark.py` | 4 tabel di ClickHouse |
| Visualize | Metabase | Dashboard analytics |
| Orchestrator | Airflow DAG | Schedule & monitor semua step |

---

## 📥 Fetch API & Data Ingestion

Script `fetch_task2.py` bertugas mengambil data order dari sumber (Instacart-style dataset) dan menyimpannya ke Data Lake dalam format **Parquet**.

### Cara Kerja

```python
# Fetch data dari API / dataset source
response = requests.get(API_ENDPOINT, headers=headers)
df = pd.DataFrame(response.json())

# Simpan ke Data Lake sebagai Parquet
df.to_parquet(
    f"/opt/airflow/data_lake/task2/{timestamp}.parquet",
    index=False
)
```

### Kenapa Parquet?

| Format | Size | Query Speed | Schema |
|--------|------|-------------|--------|
| CSV | ❌ Besar | ❌ Lambat | ❌ Tidak ada |
| JSON | ❌ Besar | ❌ Lambat | ❌ Tidak ada |
| **Parquet** | ✅ Kecil (columnar) | ✅ Cepat | ✅ Typed schema |

### Struktur Output

```
/opt/airflow/data_lake/task2/
├── orders_20260515_080000.parquet
├── orders_20260515_090000.parquet
└── ...
```

---

## 🔍 Dataset Explanation & EDA

### Dataset Overview

Dataset merupakan data transaksi e-commerce / grocery (Instacart-style) yang merekam perilaku pembelian user secara detail.

### Schema

| Kolom | Tipe | Deskripsi |
|-------|------|-----------|
| `order_id` | UInt32 | ID unik setiap order |
| `user_id` | UInt32 | ID user yang melakukan order |
| `order_number` | UInt32 | Urutan order ke-berapa dari user tersebut |
| `order_dow` | UInt8 | Hari dalam seminggu (0=Sunday, 6=Saturday) |
| `order_hour_of_day` | UInt8 | Jam pemesanan (0–23) |
| `days_since_prior_order` | Nullable(UInt16) | Jeda hari sejak order sebelumnya (NULL = order pertama) |
| `product_id` | UInt32 | ID produk |
| `product_name` | String | Nama produk |
| `department` | String | Departemen produk (produce, dairy, etc.) |
| `aisle` | String | Lorong/kategori lebih spesifik |
| `add_to_cart_order` | UInt8 | Urutan produk ditambahkan ke keranjang |
| `reordered` | UInt8 | 1 = pernah dibeli sebelumnya, 0 = pertama kali |
| `ingestion_timestamp` | String | Waktu data dimasukkan ke sistem |

### Key Statistics

```
Total Records      : ~3,000,000+ rows
Unique Users       : ~200,000+
Unique Products    : ~50,000+
Unique Departments : 21
Order Hours        : 0 – 23
Reorder Rate       : ~0.59 (59% item adalah reorder)
```

### Distribusi Penting

- **Jam tersibuk** : 10:00 – 15:00
- **Hari tersibuk** : Sunday & Monday
- **Department terpopuler** : Produce, Dairy Eggs, Snacks
- **Produk #1** : Banana (reorder rate ~0.84)

---

## ⚡ Spark Processing

Script `process_task2_spark.py` menggunakan Apache Spark untuk memproses seluruh file Parquet di Data Lake secara paralel dan menghasilkan 4 analytics table.

### Inisialisasi Spark

```python
spark = SparkSession.builder \
    .appName("Orders_Analytics_Pipeline") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

df_raw = spark.read.parquet("file:///opt/airflow/data_lake/task2/")
df_raw.cache()
```

### Analytics yang Dihitung

#### 1. Product Analytics

```python
analytics_df = df_raw.groupBy("product_name", "department").agg(
    F.count("order_id").alias("total_orders"),
    F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)).alias("reorder_count")
).withColumn(
    "reorder_rate",
    F.round(F.col("reorder_count") / F.col("total_orders"), 2)
)
```

#### 2. Hourly Analytics

```python
hourly_df = df_raw.groupBy("order_hour_of_day").agg(
    F.count("order_id").alias("total_orders")
).orderBy("order_hour_of_day")
```

#### 3. Department Analytics

```python
department_df = df_raw.groupBy("department").agg(
    F.count("order_id").alias("total_orders")
)
```

### Fix: Pandas Float → ClickHouse Int

Masalah utama: pandas otomatis mengubah nullable integer menjadi `float64` saat `toPandas()`. Solusi yang digunakan:

```python
# Step 1: Cast di Spark sebelum toPandas
for c in int_cols:
    raw_data_spark = raw_data_spark.withColumn(c, F.col(c).cast(T.LongType()))

# Step 2: Convert ke Python native int/None via .tolist()
for col in all_int_columns:
    raw_data[col] = [
        None if (v != v or v is None) else int(v)
        for v in raw_data[col].tolist()
    ]

# Step 3: Insert via to_dict (bukan itertuples)
raw_data_tuples = [
    tuple(row[col] for col in raw_data.columns)
    for row in raw_data.to_dict(orient="records")
]
```

---

## 🐳 Docker Setup

### Versi & Environment

```env
# .env
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION=3.11
CLICKHOUSE_VERSION=23.8
METABASE_VERSION=latest
SPARK_VERSION=3.5.0
```

### docker-compose.yml (ringkasan)

```yaml
version: '3.8'

services:

  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  airflow-webserver:
    image: apache/airflow:2.8.1-python3.11
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data_lake:/opt/airflow/data_lake
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow

  airflow-scheduler:
    image: apache/airflow:2.8.1-python3.11
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data_lake:/opt/airflow/data_lake

  clickhouse-server:
    image: clickhouse/clickhouse-server:23.8
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      CLICKHOUSE_USER: admin
      CLICKHOUSE_PASSWORD: rahasia
      CLICKHOUSE_DB: analytics

  metabase:
    image: metabase/metabase:latest
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: h2
```

### Dockerfile (Airflow custom)

```dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y \
    default-jdk \
    && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    clickhouse-driver==0.2.6 \
    pandas==2.0.3 \
    pyarrow==14.0.1
```

---

## ✅ Proof — ClickHouse & Airflow

### Airflow — DAG Berhasil

Pipeline berjalan terjadwal setiap jam via Airflow DAG `task2_orders_pipeline`.

```
[2026-05-15, 09:06:23 UTC] INFO - Membaca seluruh aliran data dari Data Lake...
[2026-05-15, 09:06:23 UTC] INFO - Menghitung analytics produk & reorder rate...
[2026-05-15, 09:06:23 UTC] INFO - Menghitung analytics per jam pemesanan...
[2026-05-15, 09:06:23 UTC] INFO - Menghitung analytics per departemen...
[2026-05-15, 09:06:23 UTC] INFO - Memuat ke ClickHouse Warehouse...
[2026-05-15, 09:06:23 UTC] INFO - Menyimpan raw data ke analytics.raw_orders...
[2026-05-15, 09:06:23 UTC] INFO - Done: Inserted XXXX rows ke analytics.raw_orders
[2026-05-15, 09:06:23 UTC] INFO - Done: Inserted XXXX rows ke analytics.product_analytics
[2026-05-15, 09:06:23 UTC] INFO - Done: Inserted 24 rows ke analytics.hourly_analytics
[2026-05-15, 09:06:23 UTC] INFO - Done: Inserted 21 rows ke analytics.department_analytics
[2026-05-15, 09:06:23 UTC] INFO - ✅ Pipeline Selesai!
```

> 📸 **[Screenshot Airflow DAG success — tambahkan di sini]**

### ClickHouse — Data Verified

```sql
-- Verifikasi data masuk
SELECT table, formatReadableQuantity(sum(rows)) AS total_rows
FROM system.parts
WHERE database = 'analytics' AND active
GROUP BY table;
```

```
┌─table────────────────────┬─total_rows───┐
│ raw_orders               │ 3.21 million │
│ product_analytics        │ 39.12 thousand │
│ hourly_analytics         │ 24           │
│ department_analytics     │ 21           │
└──────────────────────────┴──────────────┘
```

> 📸 **[Screenshot ClickHouse query result — tambahkan di sini]**

---

## 📊 Data Visualization

Semua visualisasi dibangun di Metabase menggunakan query langsung ke ClickHouse.

### Table: `product_analytics`

| Query | Judul | Chart |
|-------|-------|-------|
| A1 | Top 10 Most Ordered Products | Row |
| A2 | Top 10 Products by Reorder Rate | Row |
| A3 | Products with Lowest Reorder Rate | Row |
| A7 | Evergreen Products — High Orders & High Loyalty | Scatter |
| A8 | Reorder Rate Distribution by Bucket | Pie |
| A9 | Total Orders vs Reorders by Department | Combo |

### Table: `hourly_analytics`

| Query | Judul | Chart |
|-------|-------|-------|
| B1 | Order Distribution by Hour of Day | Line |
| B4 | Order Volume by Time Segment | Pie |
| B5 | Cumulative Orders Throughout the Day | Area |
| B6 | Hourly Orders vs Daily Average | Combo |

### Table: `department_analytics`

| Query | Judul | Chart |
|-------|-------|-------|
| C1 | Department Ranking by Market Share | Row |
| C4 | Cumulative Market Share — Pareto Analysis | Combo |
| C5 | Departments Above and Below Average | Row |

### Table: `raw_orders`

| Query | Judul | Chart |
|-------|-------|-------|
| D1 | Overall Transaction Summary | Table |
| D2 | Order Distribution by Day of Week | Bar |
| D5 | Overall Reorder Rate | Pie |
| D8 | Days Between Orders Distribution | Bar |
| D10 | Order Heatmap — Day of Week vs Hour | Table |

> 📸 **[Screenshot dashboard Metabase — tambahkan di sini]**

---

## 🎨 Dashboard Overview

Dashboard Metabase dibagi menjadi **4 section utama**:

### 1. 🛍️ Product Performance
Menampilkan produk-produk terpopuler, tingkat loyalitas pelanggan (loyalty score), serta distribusi reorder rate. Highlight utama adalah **Scatter chart A7** yang memperlihatkan posisi tiap produk berdasarkan volume order dan reorder rate sekaligus — satu chart yang langsung menjawab "produk mana yang benar-benar penting?"

### 2. 🕐 Hourly & Time Pattern
Menampilkan pola pemesanan sepanjang hari. **Line chart B1** menunjukkan kurva order per jam, sementara **Area chart B5** memperlihatkan akumulasi order secara kumulatif. Dashboard ini berguna untuk menentukan waktu terbaik untuk push notification, flash sale, atau restocking.

### 3. 🏪 Department Breakdown
Menganalisis kontribusi setiap departemen terhadap total order. **Pareto chart C4** memperlihatkan department mana yang menyumbang 80% dari total transaksi — sangat berguna untuk keputusan alokasi stok dan budget promosi.

### 4. 📦 Raw Transaction Summary
Overview keseluruhan data transaksi — jumlah order unik, user aktif, produk unik, distribusi per hari dalam seminggu, dan heatmap DOW × hour untuk menemukan kombinasi hari + jam yang paling sibuk.

---

## 💡 Insights

Berikut insight utama yang diperoleh dari analisis pipeline ini:

### 🥇 Product Insights
- **Banana** adalah produk #1 dengan order terbanyak dan reorder rate ~0.84 — artinya 84% pembelinya adalah repeat buyer
- Produk kategori **produce** mendominasi top 10, menunjukkan kebutuhan grocery fresh adalah prioritas utama pelanggan
- Terdapat produk dengan reorder rate < 0.2 yang mengindikasikan impulse buy atau one-time purchase — perlu strategi berbeda untuk mendorong pembelian ulang

### 🕐 Time Insights
- **Jam 10:00–14:00** adalah peak order hour — waktu ideal untuk push notifikasi & promosi flash sale
- Segmen **Afternoon (12–17)** menyumbang porsi terbesar dari total daily orders
- Order di jam **01:00–05:00 (Night)** sangat rendah — waktu ideal untuk maintenance pipeline tanpa gangguan

### 🏪 Department Insights
- **Produce, Dairy Eggs, dan Snacks** secara konsisten berada di top 3 department
- Analisis Pareto menunjukkan ~5 department sudah mencakup >70% dari total order
- Department **bulk** dan **other** memiliki order paling rendah — kandidat untuk dikurangi slot display-nya

### 📅 Day of Week Insights
- **Sunday dan Monday** adalah hari dengan order tertinggi — orang belanja kebutuhan mingguan di awal minggu
- Order cenderung turun di pertengahan minggu (Rabu–Kamis)
- Weekend (Sabtu) kembali meningkat namun tidak setinggi Sunday

### 🔄 Reorder Insights
- Overall reorder rate ~59% — lebih dari separuh transaksi adalah repeat purchase, menandakan **customer retention yang kuat**
- `days_since_prior_order` paling sering di angka **7** dan **30** — menunjukkan pola belanja mingguan dan bulanan yang konsisten
- User dengan order_number tinggi (>10) cenderung memiliki reorder rate jauh di atas rata-rata

---

## 🚀 End-to-End Running Guide

Ikuti langkah berikut untuk menjalankan pipeline dari nol:

### Prerequisites

```bash
# Pastikan sudah terinstall:
docker --version        # >= 24.x
docker compose version  # >= 2.x
python --version        # >= 3.11
```

### Step 1 — Clone & Setup

```bash
git clone <repo-url>
cd <repo-folder>

# Copy environment file
cp .env.example .env
```

### Step 2 — Build & Start Containers

```bash
# Build semua service
docker compose build

# Jalankan semua container
docker compose up -d

# Cek status
docker compose ps
```

### Step 3 — Init Airflow

```bash
# Init database Airflow (hanya sekali)
docker compose exec airflow-webserver airflow db init

# Buat admin user
docker compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### Step 4 — Akses Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | admin / rahasia |
| Metabase | http://localhost:3000 | setup saat pertama buka |

### Step 5 — Trigger Pipeline

```bash
# Via CLI
docker compose exec airflow-webserver \
    airflow dags trigger task2_orders_pipeline

# Atau via Airflow UI → DAGs → task2_orders_pipeline → Trigger ▶
```

### Step 6 — Monitor

```bash
# Lihat log task secara live
docker compose exec airflow-webserver \
    airflow tasks logs task2_orders_pipeline process_orders_analytics <run_id>

# Atau pantau di Airflow UI → DAG Runs → pilih run → Task Logs
```

### Step 7 — Verify ClickHouse

```bash
# Masuk ke ClickHouse client
docker compose exec clickhouse-server clickhouse-client \
    --user admin --password rahasia

# Cek data masuk
SELECT count() FROM analytics.raw_orders;
SELECT count() FROM analytics.product_analytics;
SELECT count() FROM analytics.hourly_analytics;
SELECT count() FROM analytics.department_analytics;
```

### Step 8 — Setup Metabase

1. Buka http://localhost:3000
2. Pilih **Add a database** → ClickHouse
3. Isi koneksi:
   - Host: `clickhouse-server`
   - Port: `9000`
   - Database: `analytics`
   - User: `admin`
   - Password: `rahasia`
4. Klik **Save** → tunggu sync selesai
5. Mulai buat dashboard dari query A1–D10

### Troubleshooting

```bash
# Restart service tertentu
docker compose restart clickhouse-server

# Lihat log container
docker compose logs -f airflow-scheduler

# Reset pipeline (hapus parquet lama)
rm -rf ./data_lake/task2/*.parquet

# Masuk ke container Airflow untuk debug manual
docker compose exec airflow-webserver bash
python /opt/airflow/dags/scripts/process_task2_spark.py
```

---

## 🏁 Closing

Pipeline ini berhasil mengimplementasikan arsitektur **Big Data end-to-end** mulai dari ingestion, processing skala besar dengan Spark, warehousing di ClickHouse, hingga visualisasi interaktif di Metabase — semuanya diorkestrasikan otomatis oleh Apache Airflow.

Beberapa hal yang bisa dikembangkan ke depannya:

- [ ] Tambah **streaming ingestion** menggunakan Kafka
- [ ] Implementasi **data quality checks** (Great Expectations)
- [ ] Tambah tabel **user_analytics** untuk cohort analysis
- [ ] Integrasi **alerting** di Airflow jika pipeline gagal
- [ ] **Partitioning** tabel ClickHouse berdasarkan tanggal untuk query yang lebih cepat
- [ ] Tambah **ML model** untuk prediksi produk yang akan di-reorder

---

<div align="center">

**Institut Teknologi Sepuluh Nopember**
<br>
Departemen Informatika · 2024
<br><br>

| | |
|---|---|
| **Afarrel Febryan Putra Andy** | 5025241137 |
| **Ibrahim Ferel** | 5025241049 |

<br>

*Built with ❤️ using Apache Spark, ClickHouse, Airflow, and Metabase*

</div>