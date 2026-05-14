import pandas as pd
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
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/task2/")
    # df_raw = spark.read.parquet("data_lake/task2/")
    df_raw.cache()

    print("Menghitung analytics produk & reorder rate...")
    analytics_df = df_raw.groupBy(
            "product_name",
            "department"
        ).agg(
            F.count("order_id").alias("total_orders"),

            F.sum(F.when(F.col("reordered") == 1, 1).otherwise(0)).alias("reorder_count")
        ).withColumn(
            "reorder_rate",
            F.round(
                F.when(
                    F.col("total_orders") > 0,
                    F.col("reorder_count") / F.col("total_orders")
                ).otherwise(0),
                2
            )
        ).orderBy(
            F.desc("total_orders")
        )

    analytics_results = analytics_df.toPandas()

    print("Menghitung analytics per jam pemesanan...")
    hourly_df = df_raw.groupBy(
            "order_hour_of_day"
        ).agg(
            F.count("order_id").alias("total_orders")
        ).orderBy(
            "order_hour_of_day"
        )
    
    hourly_results = hourly_df.toPandas()

    print("Menghitung analytics per departemen...")
    department_df = df_raw.groupBy(
            "department"
        ).agg(
            F.count("order_id").alias("total_orders")
        )
    
    department_results = department_df.toPandas()

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
        CREATE TABLE IF NOT EXISTS analytics.raw_orders (
            order_id UInt32,
            user_id UInt32,
            order_number UInt32,
            order_dow UInt8,
            order_hour_of_day UInt8,
            days_since_prior_order Nullable(UInt16),
            product_id UInt32,
            product_name String,
            department String,
            aisle String,
            add_to_cart_order UInt8,
            reordered UInt8,
            ingestion_timestamp String
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
    ''')
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.product_analytics (
            product_name String,
            department String,
            total_orders UInt32,
            reorder_count UInt32,
            reorder_rate Float64
        ) ENGINE = MergeTree()
        ORDER BY (product_name, department)
    ''')
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.hourly_analytics (
            order_hour_of_day UInt8,
            total_orders UInt32
        ) ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    ''')
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.department_analytics (
            department String,
            total_orders UInt32
        ) ENGINE = MergeTree()
        ORDER BY department
    ''')
    
    print(analytics_results.head())

    # Mode Overwrite (Truncate & Insert) untuk raw_orders
    print("Menyimpan raw data ke analytics.raw_orders...")
    client.execute('TRUNCATE TABLE analytics.raw_orders')
    raw_data = df_raw.select(
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "product_id",
        "product_name",
        "department",
        "aisle",
        "add_to_cart_order",
        "reordered",
        "ingestion_timestamp"
    ).toPandas()

    raw_data = raw_data.where(pd.notnull(raw_data), None)
    
    raw_data_tuples = [tuple(x) for x in raw_data.to_numpy()]
    if raw_data_tuples:
        client.execute('INSERT INTO analytics.raw_orders VALUES', raw_data_tuples)
    print(f"Done: Inserted {len(raw_data_tuples)} rows ke analytics.raw_orders")

    # Mode Overwrite (Truncate & Insert) agar dasbor Metabase selalu fresh
    client.execute('TRUNCATE TABLE analytics.product_analytics')
    data_tuples = [tuple(x) for x in analytics_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.product_analytics VALUES', data_tuples)
    print(f"Done: Inserted {len(data_tuples)} rows ke analytics.product_analytics")

    # Mode Overwrite (Truncate & Insert) untuk hourly_analytics
    print("Menyimpan analytics per jam ke analytics.hourly_analytics...")
    client.execute('TRUNCATE TABLE analytics.hourly_analytics')
    hourly_tuples = [tuple(x) for x in hourly_results.to_numpy()]
    if hourly_tuples:
        client.execute('INSERT INTO analytics.hourly_analytics VALUES', hourly_tuples)
    print(f"Done: Inserted {len(hourly_tuples)} rows ke analytics.hourly_analytics")

    # Mode Overwrite (Truncate & Insert) untuk department_analytics
    print("Menyimpan analytics per departemen ke analytics.department_analytics...")
    client.execute('TRUNCATE TABLE analytics.department_analytics')
    department_tuples = [tuple(x) for x in department_results.to_numpy()]
    if department_tuples:
        client.execute('INSERT INTO analytics.department_analytics VALUES', department_tuples)
    print(f"Done: Inserted {len(department_tuples)} rows ke analytics.department_analytics")
    
    # Menghapus file .parquet yang sudah diproses agar tidak menumpuk
    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/task2/*.parquet')
    # files = glob.glob('data_lake/task2/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")
    
    print("✅ Pipeline Selesai!")
    spark.stop()

if __name__ == "__main__":
    run_spark_analytics()