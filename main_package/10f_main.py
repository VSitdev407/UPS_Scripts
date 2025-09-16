import requests

from requests.auth import HTTPBasicAuth

import pandas as pd

from io import StringIO

import os

import time

from datetime import datetime
 
# UPS 10F 設定

ups_name = "UPS_10F"

ip = "172.21.2.13"

username = "admin"

password = "misadmin"

target_date = datetime.today().strftime("%Y%m%d")
 
# 儲存路徑

onelake_path = r"C:\\Users\\itdev\\OneLake - Microsoft\\global-IT-DEV\\selena_lakehouse.Lakehouse\\Files"
tst_path = r"E:\\UPS_Scripts"

output_dir = os.path.join(onelake_path, "ups_data_all")
tst_output_dir = os.path.join(tst_path, "ups_logs")

os.makedirs(output_dir, exist_ok=True)
os.makedirs(tst_output_dir, exist_ok=True)

output_file = os.path.join(output_dir, f"{ups_name}.csv")
tst_file = os.path.join(tst_output_dir, f"{ups_name}.csv")
 
# 建立 session 並登入

session = requests.Session()

session.auth = HTTPBasicAuth(username, password)
 
try:

    print(f"📡 正在觸發日期 {target_date} 資料準備...")

    # ✅ 模擬使用者在下拉式選單選擇日期（注意是 `$data_date`）

    session.get(f"http://{ip}/refresh_data.cgi", params={"$data_date": target_date}, timeout=10)

    time.sleep(3)  # 等待資料準備
 
    # ✅ 下載資料

    response = session.post(

        f"http://{ip}/download.cgi",

        data={"$data_date": target_date},

        headers={

            "User-Agent": "Mozilla/5.0",

            "Referer": f"http://{ip}"

        },

        timeout=10

    )
 
    if response.status_code == 200 and len(response.content) > 100:

        print(f"📥 成功下載資料，開始解析...")

        decoded = response.content.decode("utf-8", errors="ignore")

        df = pd.read_csv(StringIO(decoded), header=None, skiprows=1)
 
        # 標準化欄位

        df.columns = ["DateTime", "Vin", "Vout", "Freq", "Load", "Capacity", "Vbat", "CellVolt", "Temp"]

        df = df.dropna(subset=["DateTime"])

        df[["Date", "Time"]] = df["DateTime"].str.strip().str.split(" ", expand=True, n=1)

        df.drop(columns=["DateTime", "Capacity", "CellVolt"], inplace=True)

        df["Fin"] = df["Freq"]

        df["Fout"] = df["Freq"]

        df["Temp"] = df["Temp"].str.extract(r"([\d\.]+)").astype(float)
 
        # 重排欄位順序並標註 UPS 名稱

        df = df[["Date", "Time", "Vin", "Vout", "Vbat", "Fin", "Fout", "Load", "Temp"]]

        df["UPS_Name"] = ups_name

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
 
        # ✅ 合併已存在資料

        if os.path.exists(output_file):

            existing = pd.read_csv(output_file)

            combined = pd.concat([existing, df], ignore_index=True)

            combined.drop_duplicates(subset=["Date", "Time", "UPS_Name"], inplace=True)

        else:

            combined = df
 
        combined.to_csv(output_file, index=False)
        combined.to_csv(tst_file, index=False)

        print(f"✅ 資料寫入成功：{output_file}，總筆數：{len(combined)}")
        print(f"✅ 資料寫入成功：{tst_file}，總筆數：{len(combined)}")
 
    else:

        print(f"❌ 資料下載失敗，狀態碼 {response.status_code}，回傳長度：{len(response.content)}")
 
except Exception as e:

    print(f"⚠️ 發生錯誤：{e}")

 