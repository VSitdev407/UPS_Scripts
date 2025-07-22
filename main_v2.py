import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO
import os
from datetime import datetime

# UPS 列表設定（含樓層/IP/type）
ups_targets = [
    {"name": "UPS_3F", "ip": "172.21.5.14", "type": "standard"},
    {"name": "UPS_7F", "ip": "172.21.6.10", "type": "standard"},
    {"name": "UPS_8F", "ip": "172.21.4.10", "type": "standard"},
    {"name": "UPS_9F", "ip": "172.21.3.11", "type": "standard"},
    {"name": "UPS_10F", "ip": "172.21.2.13", "type": "new"}  # 特別處理
]

username = "admin"
password = "misadmin"

# OneLake 路徑設定
onelake_path = r"C:\\Users\\itdev\\OneLake - Microsoft\\global-IT-DEV\\selena_lakehouse.Lakehouse\\Files"
output_dir = os.path.join(onelake_path, "ups_data_all")
os.makedirs(output_dir, exist_ok=True)

log_file = os.path.join(output_dir, "ups_error_log.csv")
error_logs = []

target_date = datetime.today().strftime("%Y%m%d")

for ups in ups_targets:
    ups_name = ups["name"]
    ip = ups["ip"]
    ups_type = ups["type"]
    print(f"\U0001f4f1 正在下載 {ups_name} 的資料...")

    try:
        session = requests.Session()
        session.auth = HTTPBasicAuth(username, password)

        if ups_type == "standard":
            session.get(f"http://{ip}", timeout=10)
            url = f"http://{ip}/cgi-bin/datalog.csv?page=421&"
            form_data = {"GETDATFILE": "Download"}
        else:
            session.get(f"http://{ip}/refresh_data.cgi", params={"data_date": target_date}, timeout=10)
            url = f"http://{ip}/download.cgi"
            form_data = {"data_date": target_date}

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": f"http://{ip}"
        }

        response = session.post(url, headers=headers, data=form_data, timeout=10)

        if response.status_code == 200 and len(response.content) > 100:
            decoded = response.content.decode("utf-8", errors="ignore")
            df = pd.read_csv(StringIO(decoded), header=None, skiprows=1)

            # 欄位標準化
            if ups_type == "standard":
                df.columns = ["Date", "Time", "Vin", "Vout", "Vbat", "Fin", "Fout", "Load", "Temp"]
            else:
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

            # 儲存資料
            combined_path = os.path.join(output_dir, f"{ups_name}.csv")
            if os.path.exists(combined_path):
                existing = pd.read_csv(combined_path)
                combined = pd.concat([existing, df], ignore_index=True)
                combined.drop_duplicates(subset=["Date", "Time", "UPS_Name"], inplace=True)
            else:
                combined = df

            combined.to_csv(combined_path, index=False)
            print(f"{ups_name} 寫入 {combined_path}，總筆數：{len(combined)}")

        else:
            msg = f"{ups_name} 下載失敗：狀態碼 {response.status_code}"
            print(msg)
            error_logs.append({"UPS": ups_name, "IP": ip, "Date": target_date, "Error": msg})

    except Exception as e:
        msg = f"{ups_name} 例外錯誤：{str(e)}"
        print(msg)
        error_logs.append({"UPS": ups_name, "IP": ip, "Date": target_date, "Error": str(e)})

# 儲存錯誤紀錄
if error_logs:
    pd.DataFrame(error_logs).to_csv(r"E:\UPS_Scripts\ups_logs\ups_error_log.csv", index=False)
    print(f"錯誤已記錄至 {log_file}")
else:
    print("所有 UPS 資料成功下載無錯誤")
