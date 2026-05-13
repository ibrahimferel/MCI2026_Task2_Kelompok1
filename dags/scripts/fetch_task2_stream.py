import requests
import pandas as pd
import os
from datetime import datetime


def fetch_task2_orders():
    print("📦 Mengambil data dari Orders API...")
    url = "http://96.9.212.102:8000/orders"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        orders = data["orders"]

        parsed_data = []

        for order in orders:
            for product in order.get("products", []):
                parsed_data.append({
                    "order_id": order.get("order_id"),
                    "user_id": order.get("user_id"),
                    "order_number": order.get("order_number"),
                    "order_dow": order.get("order_dow"),
                    "order_hour_of_day": order.get("order_hour_of_day"),
                    "days_since_prior_order": order.get("days_since_prior_order"),

                    "product_id": product.get("product_id"),
                    "product_name": product.get("product_name"),
                    "department": product.get("department"),
                    "aisle": product.get("aisle"),
                    "add_to_cart_order": product.get("add_to_cart_order"),
                    "reordered": product.get("reordered"),
                    
                    "ingestion_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        # Convert ke DataFrame
        df = pd.DataFrame(parsed_data)

        # Validasi data kosong
        if df.empty:
            raise Exception("DataFrame kosong, tidak ada data yang diproses")

        # Path output parquet
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        # output_dir = "/opt/airflow/data_lake/task2"
        output_dir = "data_lake/task2"

        os.makedirs(output_dir, exist_ok=True)

        output_path = f"{output_dir}/orders_{current_time}.parquet"

        # Simpan parquet
        df.to_parquet(output_path, index=False)

        print(f"✅ Berhasil menyimpan {len(df)} baris ke:")
        print(output_path)

    except Exception as e:
        print(f"❌ Gagal mengambil data: {e}")
        raise


if __name__ == "__main__":
    fetch_task2_orders()