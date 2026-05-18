<div align="center">

# Grocery Orders Analytics Dashboard - Pipeline Detailing Explanation

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-latest-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![Metabase](https://img.shields.io/badge/Metabase-latest-509EE3?style=flat-square&logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

</div>

---

## Table of Contents

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

### Workflow

```text
Orders API
    ↓
HTTP Request (GET)
    ↓
JSON Response
    ↓
Nested Data Parsing
    ↓
Feature Extraction
    ↓
Flattening Orders + Products
    ↓
Add Ingestion Timestamp
    ↓
Save as Parquet
```

### Langkah Kerja

1. **Mengambil data dari Orders API**

   Script melakukan HTTP GET request ke endpoint:

   ```text
   http://96.9.212.102:8000/orders
   ```

   untuk memperoleh dataset transaksi pesanan.

2. **Parsing JSON Response**

   Response API dikonversi menjadi Python dictionary menggunakan:

   ```python
   response.json()
   ```

   Dataset memiliki struktur **nested JSON**, di mana setiap order menyimpan metadata transaksi dan daftar produk di dalam key `products`.

   Contoh struktur data:

   ```json
   {
     "order_id": 1,
     "user_id": 5,
     "products": [
       {
         "product_id": 10,
         "product_name": "Banana"
       }
     ]
   }
   ```

3. **Feature Extraction dari Nested JSON**

   Karena dataset bersifat nested, pipeline melakukan ekstraksi fitur dari dua level data:

   **A. Order-level features**

   Metadata utama transaksi yang diambil dari objek order:

   - `order_id`
   - `user_id`
   - `order_number`
   - `order_dow`
   - `order_hour_of_day`
   - `days_since_prior_order`

   **B. Product-level features**

   Informasi produk yang diambil dari list `products`:

   - `product_id`
   - `product_name`
   - `department`
   - `aisle`
   - `add_to_cart_order`
   - `reordered`

4. **Flatten Nested Structure**

   Karena satu order dapat memiliki banyak produk, dataset diubah dari bentuk nested menjadi bentuk tabular.

   Contoh:

   Sebelum flattening:

   ```text
   Order 1
    └── Products:
         ├── Banana
         ├── Milk
         └── Bread
   ```

   Setelah flattening:

   | order_id | product_name |
   |----------|---------------|
   | 1 | Banana |
   | 1 | Milk |
   | 1 | Bread |

   Proses ini mempermudah analytics menggunakan Spark SQL dan penyimpanan ke warehouse.

5. **Menambahkan Ingestion Timestamp**

   Setiap row diberikan atribut:

   ```text
   ingestion_timestamp
   ```

   untuk mencatat waktu data diambil oleh pipeline.

6. **Membuat DataFrame**

   Data hasil parsing dan flattening dikonversi menjadi **Pandas DataFrame**.

7. **Menyimpan ke Data Lake**

   Dataset disimpan ke:

   ```text
   /opt/airflow/data_lake/task2/
   ```

   menggunakan format **Apache Parquet**.

### Final Dataset Schema

| Feature |
|----------|
| order_id |
| user_id |
| order_number |
| order_dow |
| order_hour_of_day |
| days_since_prior_order |
| product_id |
| product_name |
| department |
| aisle |
| add_to_cart_order |
| reordered |
| ingestion_timestamp |

### Output

Contoh output file:

```text
orders_20260517_143022.parquet
```

Format **Apache Parquet** dipilih karena:

- columnar storage
- kompresi lebih efisien
- optimal untuk pemrosesan Spark
- umum digunakan dalam pipeline Data Engineering

### Struktur Output

Sebelum pipeline diorkestrasi menggunakan Apache Airflow, setiap tahapan proses diuji terlebih dahulu melalui terminal untuk melakukan validasi fungsi, alur data, dan output yang dihasilkan:

![alt text](asset_documentation/image-1.png)

Contoh Output:

![alt text](asset_documentation/image-2.png)

---

## 🔍 Dataset Explanation 

### Dataset Overview

Dataset merupakan data transaksi e-commerce / grocery (Instacart-style) yang merekam perilaku pembelian user secara detail.

### Visualisasi Struktur Nested JSON Dataset

![alt text](asset_documentation/image-3.png)

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

### Preview Isi File Parquet

![alt text](asset_documentation/image-4.png)

---

## ⚡ Spark Processing

Script `process_task2_spark.py` menggunakan Apache Spark untuk memproses seluruh file Parquet di Data Lake secara paralel dan menghasilkan 4 analytics table.

#### Workflow

```text
Parquet Files (Data Lake)
        ↓
Spark Read Parquet
        ↓
Data Processing & Aggregation
        ↓
Generate Analytics Tables
        ↓
Data Type Validation & Casting
        ↓
Load to ClickHouse Warehouse
        ↓
Cleanup Processed Parquet Files
```

### 1. Membaca Data dari Data Lake

Pipeline membaca seluruh file parquet yang tersedia pada folder:

```text
/opt/airflow/data_lake/task2/
```

menggunakan Apache Spark.

```python
spark.read.parquet(...)
```

Karena Spark mendukung pembacaan multi-file secara langsung, seluruh batch data dapat diproses sekaligus.

Dataset kemudian di-cache menggunakan:

```python
df_raw.cache()
```

untuk meningkatkan performa pada proses analytics yang menggunakan dataframe yang sama berulang kali.

### 2. Product Analytics Processing

Pipeline melakukan agregasi analytics berdasarkan:

- `product_name`
- `department`

Metrik yang dihitung:

| Metric | Description |
|---------|-------------|
| total_orders | Total jumlah transaksi produk |
| reorder_count | Total produk yang di-reorder |
| reorder_rate | Persentase reorder terhadap total transaksi |

Formula reorder rate:

```text
reorder_rate = reorder_count / total_orders
```

Contoh insight yang dapat digali:

- Produk paling populer.
- Produk dengan tingkat pembelian ulang tertinggi.
- Performa produk per departemen.

### 3. Hourly Analytics Processing

Pipeline melakukan grouping berdasarkan:

```text
order_hour_of_day
```

untuk memperoleh distribusi jumlah transaksi per jam.

Metrik:

- `total_orders`

Insight yang dapat diperoleh:

- Jam tersibuk pelanggan melakukan pemesanan.
- Pola aktivitas pembelian harian.

### 4. Department Analytics Processing

Analytics departemen dihitung melalui grouping:

```text
department
```

untuk memperoleh total volume transaksi tiap departemen.

Insight:

- Departemen dengan aktivitas pembelian tertinggi.
- Distribusi penjualan antar kategori produk.

### 5. ClickHouse Data Warehouse Initialization

Pipeline melakukan koneksi ke ClickHouse Warehouse.

Database analytics dibuat secara otomatis apabila belum tersedia:

```sql
CREATE DATABASE IF NOT EXISTS analytics
```

Kemudian dibuat empat tabel warehouse:

| Table | Function |
|-------|----------|
| raw_orders | Menyimpan raw transactional dataset |
| product_analytics | Menyimpan analytics produk |
| hourly_analytics | Menyimpan analytics per jam |
| department_analytics | Menyimpan analytics per departemen |

### 6. Data Type Validation & Safe Casting

Sebelum proses insert ke ClickHouse, pipeline melakukan validasi tipe data.

Beberapa helper function digunakan:

#### safe_int()

Digunakan untuk:

- menangani nilai NULL
- menghindari type mismatch
- memastikan seluruh numeric column menjadi Python native integer.

#### safe_str()

Digunakan untuk:

- menangani nilai kosong
- memastikan seluruh string column valid sebelum insert.

Tahap ini penting karena ClickHouse memiliki validasi tipe data yang ketat.

### 7. Loading Data ke Warehouse

Pipeline menggunakan mode:

```text
TRUNCATE + INSERT
```

agar dashboard selalu menampilkan data terbaru.

Flow:

```text
TRUNCATE TABLE
      ↓
Prepare tuples
      ↓
INSERT INTO ClickHouse
```

Setiap analytics table di-overwrite menggunakan batch terbaru.

### 8. Cleanup Data Lake

Setelah proses warehouse selesai, file parquet lama dihapus otomatis:

```text
/opt/airflow/data_lake/task2/*.parquet
```

Tujuan cleanup:

- mencegah penumpukan batch lama
- menjaga storage tetap efisien
- menghindari duplicate processing.


### Output Warehouse Tables

#### 1. analytics.raw_orders

Raw transactional dataset.

#### 2. analytics.product_analytics

Analytics produk dan reorder behavior.

#### 3. analytics.hourly_analytics

Analytics distribusi transaksi per jam.

#### 4. analytics.department_analytics

Analytics volume transaksi per departemen.

---

## 🐳 Docker Setup 

### Versions & Environment

```env
AIRFLOW_VERSION=2.9.1
PYTHON_VERSION=3.11
SPARK_VERSION=3.5.0
CLICKHOUSE_VERSION=23.8
POSTGRES_VERSION=13
METABASE_VERSION=latest
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

## Proof: ClickHouse & Airflow

### Airflow: DAG Berhasil

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

![alt text](asset_documentation/image-5.png)
![alt text](asset_documentation/image-6.png)

### ClickHouse: Data Verified

```sql
-- Verifikasi data masuk
SELECT table, formatReadableQuantity(sum(rows)) AS total_rows
FROM system.parts
WHERE database = 'analytics' AND active
GROUP BY table;
```

```
┌─table────────────────────┬─total_rows───┐
│ raw_orders               │ 21           │
│ product_analytics        │ 912          │
│ hourly_analytics         │ 736          │
│ department_analytics     │ 20           │
└──────────────────────────┴──────────────┘
```

![alt text](asset_documentation/image-7.png)

---

## 📊 Data Visualization

Semua visualisasi dibangun di Metabase menggunakan query langsung ke ClickHouse.

### Table: `product_analytics`

| Query | Judul | Chart |
|-------|-------|-------|
| A1 | Top 10 Most Ordered Products | Row |
| A2 | Top 10 Products by Reorder Rate | Row |
| A3 | Products with Lowest Reorder Rate | Row |
| A4 | Unique Products & Total Orders per Department | Bar |
| A5 | Average Reorder Rate per Department | Bar |
| A6 | Top 5 Products per Department | Table |
| A7 | Evergreen Products: High Orders & High Loyalty | Scatter |
| A8 | Reorder Rate Distribution by Bucket | Pie |
| A9 | Total Orders vs Reorders by Department | Combo |
| A10 | First-Time Buyer Dominant Products | Row |

### Table: `hourly_analytics`

| Query | Judul | Chart |
|-------|-------|-------|
| B1 | Order Distributi............ | L.... |
| B2 | Order t.............. | P.... |
| B3 | Cumulative Order............. | A...... |
| B4 | Hour........................ | C........ |

### Table: `department_analytics`

| Query | Judul | Chart |
|-------|-------|-------|
| C1 | Department Ranki................. | R....... |
| C2 | Cumulative M.............. | C..... |
| C3 | Departments........... | R............ |

### Table: `raw_orders`

| Query | Judul | Chart |
|-------|-------|-------|
| D1 | Overall T................ | Ta... |
| D2 | Order Dist................ | B..... |
| D5 | Over..................... | P.... |
| D8 | Days Be..................... | B.... |
| D10 | Order Heatmap................ | T........ |

---

## 🎨 Dashboard Overview

Dashboard Metabase dibagi menjadi **4 section utama:**

### 1. Product Performance
Menyajikan analisis performa produk melalui jumlah pemesanan, perilaku reorder pelanggan, dan distribusi metrik produk. Visualisasi ini membantu memahami produk mana yang paling sering dibeli, paling konsisten di-reorder, dan paling berpengaruh dalam keseluruhan dataset.

![alt text](asset_documentation/image-8.png)
![alt text](asset_documentation/image-9.png)
![alt text](asset_documentation/image-10.png)

#### A1.     Top 10 Most Ordered Products (Row Chart)

= Menampilkan 10 produk dengan jumlah pemesanan tertinggi untuk mengidentifikasi produk paling populer dan paling sering muncul dalam transaksi. Berdasarkan hasil analisis pada dataset, daftar produk dengan volume order tertinggi didominasi oleh produk dari departemen produce. Selain itu, jumlah order pada kelompok top 10 products berada pada kisaran sekitar 7–17 transaksi, menunjukkan produk-produk dengan tingkat permintaan relatif lebih tinggi dibandingkan produk lainnya dalam dataset.

#### A2.     Top 10 Products by Reorder Rate (Row Chart)

= Menggali produk dengan tingkat pembelian ulang tertinggi sebagai indikator loyalitas atau kecenderungan pelanggan membeli kembali produk tertentu. Dari hasil analisis, terlihat adanya perbedaan komposisi ketika peringkat produk ditentukan berdasarkan reorder rate dibandingkan jumlah order. Pada kategori ini, muncul produk dari departemen beverages, yaitu Lime Sparkling Water, sementara sebagian besar produk lainnya masih relatif serupa dengan daftar produk berperforma tinggi berdasarkan total order. Hal ini menunjukkan bahwa popularitas produk dan tingkat loyalitas pelanggan tidak selalu menghasilkan peringkat yang identik.

#### A3.     Products with Lowest Reorder Rate (Row Chart)

= Menunjukkan produk dengan tingkat reorder terendah untuk membantu mengidentifikasi produk yang jarang dibeli ulang atau memiliki tingkat loyalitas pelanggan yang rendah. Berdasarkan hasil analisis, daftar produk dengan reorder rate terendah masih cukup didominasi oleh produk dari departemen produce. Namun, muncul pula beberapa produk dari departemen lain yang sebelumnya tidak menonjol pada analisis sebelumnya, seperti canned goods, frozen, dan pantry. Temuan ini mengindikasikan bahwa produk pada departemen tersebut cenderung memiliki tingkat pembelian ulang yang lebih rendah, sehingga pelanggan relatif lebih jarang melakukan pembelian kembali terhadap produk-produk tersebut.

#### A4.     Unique Products & Total Orders per Department (Bar Chart)

= Menganalisis jumlah variasi produk dan total aktivitas pemesanan pada setiap departemen untuk melihat skala serta keragaman katalog produk. Berdasarkan visualisasi yang diperoleh, departemen dengan jumlah produk unik (total products) sekaligus aktivitas pemesanan tertinggi didominasi oleh produce, diikuti oleh dairy eggs dan snacks. Hal ini menunjukkan bahwa ketiga departemen tersebut memiliki keragaman produk yang relatif besar serta tingkat permintaan pelanggan yang cukup tinggi dibandingkan departemen lainnya.

#### A5.     Average Reorder Rate per Department (Bar Chart)

= Membandingkan rata-rata tingkat reorder antar departemen untuk memahami kategori produk mana yang memiliki pola loyalitas pelanggan yang lebih tinggi. Berdasarkan hasil analisis, departemen bulk menunjukkan nilai rata-rata reorder rate tertinggi dibandingkan departemen lainnya. Namun, jumlah total orders pada kategori tersebut relatif sangat rendah, sehingga insight yang diperoleh kurang representatif untuk menggambarkan perilaku pelanggan secara umum. Jika mempertimbangkan keseimbangan antara tingkat reorder dan volume transaksi, departemen produce, dairy eggs, dan snacks menjadi kategori yang terlihat paling kuat dan konsisten dalam menunjukkan kombinasi antara aktivitas pemesanan tinggi dan loyalitas pelanggan yang baik.

#### A6.     Top 5 Products per Department (Table)

= Menampilkan lima produk terbaik pada setiap departemen berdasarkan performa order sehingga dapat terlihat produk unggulan pada masing-masing kategori. Melalui visualisasi ini, kita dapat membandingkan produk-produk teratas dari setiap departemen berdasarkan kombinasi total orders dan reorder rate. Hasil analisis menunjukkan bahwa beberapa produk memiliki performa yang menonjol di kategorinya masing-masing, dengan Lime Sparkling Water menjadi salah satu produk yang berada pada posisi teratas berdasarkan keseimbangan antara volume pemesanan dan tingkat pembelian ulang pelanggan.

#### A7.     Evergreen Products: High Orders & High Loyalty (Scatter Chart)

= Memetakan hubungan antara jumlah order dan reorder rate untuk mengidentifikasi produk yang memiliki permintaan tinggi sekaligus loyalitas pelanggan yang kuat. Berdasarkan pola yang terlihat dari analisis-analisis sebelumnya, produk dari departemen produce memang menunjukkan performa yang paling konsisten, baik dari sisi volume pemesanan maupun tingkat pembelian ulang pelanggan. Oleh karena itu, cukup wajar apabila posisi teratas pada visualisasi ini didominasi oleh produk-produk dari departemen produce, seperti Organic Strawberries, Banana, dan beberapa produk sejenis lainnya yang memiliki kombinasi kuat antara popularitas dan loyalitas pelanggan.

#### A8.     Reorder Rate Distribution by Bucket (Pie Chart)

= Melalui visualisasi pie chart reorder rate distribution, dilakukan pengelompokan tingkat reorder rate ke dalam beberapa kategori, misalnya Very Low, Low, Medium, High, dan Very High berdasarkan rentang nilai tertentu (contohnya 0.2, 0.5, 0.8, dan seterusnya). Dari hasil analisis, terlihat bahwa dataset ini memiliki cukup banyak produk dengan tingkat reorder rate tinggi, bahkan jumlahnya mencapai hampir lebih dari separuh distribusi. Menariknya, kategori dengan reorder rate sangat rendah (Very Low) juga menempati proporsi yang cukup besar. Hal ini menunjukkan adanya pola distribusi yang cukup terpolarisasi, dimana sebagian produk memiliki loyalitas pelanggan yang kuat, sementara sebagian lainnya cenderung jarang mengalami pembelian ulang.

#### A9.     Total Orders vs Reorders by Department (Combo Chart)

= Membandingkan total orders dan total reorders pada setiap departemen untuk melihat keseimbangan antara volume penjualan dan tingkat pembelian ulang pelanggan. Berdasarkan combo chart, kembali terlihat bahwa departemen produce memiliki performa yang paling seimbang dan konsisten. Departemen ini menunjukkan jumlah order yang tinggi sekaligus diikuti oleh total reorder yang juga besar, menandakan bahwa produk-produk di kategori tersebut tidak hanya sering dibeli, tetapi juga cukup berhasil mendorong pelanggan untuk melakukan pembelian ulang. Di sisi lain, beberapa departemen lain terlihat memiliki perbedaan yang cukup signifikan antara total orders dan total reorders, yang dapat mengindikasikan adanya ketimpangan antara tingkat popularitas produk dan loyalitas pelanggan.

#### A10.     First-Time Buyer Dominant Products (Row Chart)

= Melalui chart ini, kita dapat melihat produk-produk yang cukup sering dibeli pada pembelian awal, namun memiliki kecenderungan rendah untuk dibeli kembali (reorder). Menariknya, pola ini berbeda dengan analisis sebelumnya karena tidak lagi didominasi oleh departemen produce. Beberapa produk yang muncul sebagai contoh antara lain Sour Cream, Organic Zucchini, Peach Preserves, Original Spread, dan Blueberries. Hal ini menunjukkan bahwa meskipun suatu produk mampu menarik pembelian awal pelanggan, belum tentu produk tersebut berhasil mempertahankan minat pelanggan untuk melakukan pembelian ulang.

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

= Kalau dilihat secara keseluruhan, performa produk ternyata tidak sesederhana “siapa yang paling banyak dibeli” atau “siapa yang punya reorder rate paling tinggi”. Untuk mendapatkan gambaran yang lebih adil, kita perlu melihat kombinasi antara jumlah transaksi, tingkat pembelian ulang, keseimbangan performa, serta kategori departemennya.

Dari berbagai analisis yang dilakukan, departemen produce terlihat paling konsisten dalam menjaga performanya. Produk-produk pada kategori ini tidak hanya sering dibeli, tetapi juga cukup berhasil membuat pelanggan kembali membeli produk yang sama. Meski begitu, beberapa kategori lain tetap menunjukkan karakteristik uniknya masing-masing, seperti produk yang kuat di pembelian awal namun lemah di reorder, atau sebaliknya memiliki loyalitas tinggi meskipun volume ordernya tidak terlalu besar.

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

*Bismillah Admin Lab Manajemen Cerdas Informasi <3*

</div>