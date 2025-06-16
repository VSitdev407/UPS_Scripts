from datetime import datetime, timedelta
import requests
import pandas as pd
from io import StringIO
import os

# UPS 10F 設定
ups_name = "UPS_10F"
ip = "172.21.2.13"
username = "admin"
password = "misadmin"
output_dir = "ups_logs"
os.makedirs(output_dir, exist_ok=True)

# 設定時間範圍
start_date = datetime.strptime("2025-05-26", "%Y-%m-%d")
end_date = datetime.strptime("2025-06-15", "%Y-%m-%d")
date_list = [(start_date + timedelta(days=i)).strftime("%Y%m%d")
             for i in range((end_date - start_date).days + 1)]

all_data = []

for target_date in date_list:
    print(f"📅 正在處理 {target_date}...")

    session = requests.Session()
    session.auth = (username, password)

    refresh_url = f"http://{ip}/refresh_data.cgi"
    download_url = f"http://{ip}/download.cgi"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": f"http://{ip}"}
    form_data = {"$data_date": target_date}

    try:
        session.get(refresh_url, params={"data_date": target_date})
        response = session.post(download_url, data=form_data, headers=headers)
        response.raise_for_status()

        decoded = response.content.decode("utf-8", errors="ignore")
        df = pd.read_csv(StringIO(decoded), header=None, skiprows=1)

        df.columns = ["DateTime", "Vin", "Vout", "Freq", "Load", "Capacity", "Vbat", "CellVolt", "Temp"]
        df = df.dropna(subset=["DateTime"])
        df[["Date", "Time"]] = df["DateTime"].str.strip().str.split(" ", expand=True, n=1)
        df.drop(columns=["DateTime", "Capacity", "CellVolt"], inplace=True)
        df["Fin"] = df["Freq"]
        df["Fout"] = df["Freq"]
        df["Temp"] = df["Temp"].str.extract(r"([\d\.]+)").astype(float)
        df = df[["Date", "Time", "Vin", "Vout", "Vbat", "Fin", "Fout", "Load", "Temp"]]
        df["UPS_Name"] = ups_name
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

        all_data.append(df)

    except Exception as e:
        print(f"❌ 無法處理日期 {target_date}: {e}")

# 合併並儲存
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_path = f"{output_dir}/{ups_name}.csv"
    combined_df.to_csv(combined_path, index=False)
else:
    print("⚠️ 沒有成功取得任何資料")
