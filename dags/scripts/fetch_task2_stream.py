import requests
import pandas as pd
import os
from datetime import datetime

def fetch_task2_edits():
    url = "http://96.9.212.102:8000/orders"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        parsed_data = []

        orders = data["orders"]

        for order in orders:
            for product in order["products"]:
                parsed_data.append({
                    "order_id": order["order_id"],
                    "user_id": order["user_id"],
                    "order_hour": order["order_hour_of_day"],
                    "days_since_prior_order": order["days_since_prior_order"],
                    "product_id": product["product_id"],
                    "product_name": product["product_name"],
                    "department": product["department"],
                    "aisle": product["aisle"],
                    "reordered": product["reordered"]
                })

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f'/opt/airflow/data_lake/task2/edits_{current_time}.parquet'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df = pd.DataFrame(parsed_data)
        df.to_parquet(output_path, index=False)

        print("✅ Data berhasil diambil")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    fetch_task2_edits()

#     try:
#         # PERBAIKAN: Menyisipkan parameter headers ke dalam requests.get
#         response = requests.get(url, params=params, headers=headers, timeout=10)
#         response.raise_for_status()
#         data = response.json()
#         recent_changes = data['query']['recentchanges']
        
#         parsed_data = []
#         for rc in recent_changes:
#             size_diff = abs(rc.get('newlen', 0) - rc.get('oldlen', 0))
#             parsed_data.append({
#                 'edit_id': rc.get('rcid'),
#                 'title': rc.get('title'),
#                 'user': rc.get('user'),
#                 'is_bot': 'bot' in rc, 
#                 'size_diff': size_diff,
#                 'timestamp': rc.get('timestamp')
#             })
            
#         df = pd.DataFrame(parsed_data)
        
#         # Simpan ke Data Lake lokal
#         current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
#         output_path = f'/opt/airflow/data_lake/task2/edits_{current_time}.parquet'
#         os.makedirs(os.path.dirname(output_path), exist_ok=True)
#         df.to_parquet(output_path, index=False)
        
#         print(f"✅ Sukses menyimpan {len(df)} baris ke {output_path}")
#     except Exception as e:
#         print(f"❌ Gagal menarik data: {e}")
#         raise

# params = {
    #     "action": "query",
    #     "list": "recentchanges",
    #     "format": "json",
    #     "rcprop": "title|user|timestamp|sizes|flags",
    #     "rclimit": "500" # Sedot 500 suntingan terakhir per eksekusi
    # }
    # # --- PERBAIKAN MULAI DI SINI ---
    # # Menambahkan header User-Agent spesifik agar tidak diblokir Wikipedia
    # headers = {
    #     "User-Agent": "PelatihanBigDataApp/1.0 (yogasyahputra3634@email.com) Python-Requests/2.x"
    # }
    # # --- PERBAIKAN SELESAI ---
