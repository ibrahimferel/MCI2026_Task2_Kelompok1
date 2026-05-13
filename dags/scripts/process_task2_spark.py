from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Analytics_Pipeline") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    # Spark dengan mudah membaca SEMUA file parquet di folder ini sekaligus
    # df_raw = spark.read.parquet("file:///opt/airflow/data_lake/task2/")
    df_raw = spark.read.parquet("data_lake/task2/")

    print("Menghitung analytics produk & reorder rate...")
    analytics_df = df_raw.groupBy(
            "product_name",
            "department"
        ).agg(
            F.count("order_id").alias("total_orders"),

            F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)).alias("reorder_count")
        ).withColumn(
            "reorder_rate",F.round(F.col("reorder_count") / F.col("total_orders"),2)
        ).orderBy(
            F.desc("total_orders")
        )

    final_results = analytics_df.toPandas()
    spark.stop()

    print("Memuat ke ClickHouse Warehouse...")
    
    # --- PERBAIKAN MULAI DI SINI ---
    # Tambahkan parameter user dan password sesuai dengan pengaturan ClickHouse Anda
    # Jika Anda menggunakan default bawaan docker, biasanya user='default' dan password='' (kosong)
    # ATAU jika Anda mengatur password di docker-compose.yml, masukkan di sini.
    client = Client(
        host='clickhouse-server',
        user='admin',          # Ganti jika nama user Anda berbeda
        password='rahasia' # GANTI DENGAN PASSWORD CLICKHOUSE ANDA
    )
    # --- PERBAIKAN SELESAI ---

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.product_analytics (
            product_name String,
            department String,
            total_orders UInt32,
            reorder_count UInt32,
            reorder_rate Float64
        ) ENGINE = MergeTree()
        ORDER BY total_orders
    ''')
    
    print(final_results.head())
    
    # Mode Overwrite (Truncate & Insert) agar dasbor Metabase selalu fresh
    client.execute('TRUNCATE TABLE analytics.product_analytics')
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.product_analytics VALUES', data_tuples)
    
    # Menghapus file .parquet yang sudah diproses agar tidak menumpuk
    print("Membersihkan file Parquet lama dari Data Lake...")
    # files = glob.glob('/opt/airflow/data_lake/task2/*.parquet')
    files = glob.glob('data_lake/task2/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")
    
    print("✅ Pipeline Selesai!")

if __name__ == "__main__":
    run_spark_analytics()