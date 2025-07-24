import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO
import os
from datetime import datetime, timezone
import logging
import re
 
# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    filename="ups_10f_download.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)
 
# 清理檔案名稱
def clean_filename(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name)
 
# UPS_10F 設定
ups = {"name": "UPS_10F", "ip": "172.21.2.13", "type": "new"}
username = "admin"
password = "misadmin"
onelake_path = r"E:\UPS_Scripts\Temp"
output_dir = os.path.normpath(os.path.join(onelake_path, "ups_data_all"))
os.makedirs(output_dir, exist_ok=True)
 
# 檢查目錄寫入權限
if not os.access(output_dir, os.W_OK):
    logging.error(f"無法寫入目錄：{output_dir}")
    print(f"❌ 無法寫入目錄：{output_dir}")
    exit(1)
 
target_date = datetime.now(timezone.utc).strftime("%Y%m%d")
ups_name = ups["name"]
ip = ups["ip"]
ups_type = ups["type"]
print(f"📡 正在下載 {ups_name} 的資料...")
 
# 建立會話並設置認證
session = requests.Session()
session.auth = HTTPBasicAuth(username, password)
 
# UPS_10F 的請求邏輯
session.get(f"http://{ip}/refresh_data.cgi", params={"data_date": target_date})
url = f"http://{ip}/download.cgi"
form_data = {"data_date": target_date}
headers = {"User-Agent": "Mozilla/5.0", "Referer": f"http://{ip}"}
 
try:
    response = session.post(url, headers=headers, data=form_data)
    response.raise_for_status()  # 檢查 HTTP 狀態碼
 
    decoded = response.content.decode("utf-8", errors="replace")
    if len(decoded.splitlines()) < 2:
        raise ValueError("數據內容過少，可能是無效的 CSV")
 
    # 讀取並處理數據
    df = pd.read_csv(StringIO(decoded), header=None, skiprows=1)
    df.columns = ["DateTime", "Vin", "Vout", "Freq", "Load", "Capacity", "Vbat", "CellVolt", "Temp"]
    df = df.dropna(subset=["DateTime"])
    df[["Date", "Time"]] = df["DateTime"].str.strip().str.split(" ", expand=True, n=1)
    df.drop(columns=["DateTime", "Capacity", "CellVolt"], inplace=True)
    df["Fin"] = df["Freq"]
    df["Fout"] = df["Freq"]
    # 處理 Temp 欄位，確保提取數字
    df["Temp"] = df["Temp"].str.extract(r"([\d\.]+)").astype(float, errors="ignore")
    df = df[["Date", "Time", "Vin", "Vout", "Vbat", "Fin", "Fout", "Load", "Temp"]]
    df["UPS_Name"] = ups_name
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
 
    # 儲存到 CSV
    combined_path = os.path.join(output_dir, f"{clean_filename(ups_name)}_{target_date}.csv")
    if os.path.exists(combined_path):
        try:
            existing = pd.read_csv(combined_path)
            combined = pd.concat([existing, df], ignore_index=True)
            combined.drop_duplicates(subset=["Date", "Time", "UPS_Name"], inplace=True)
        except Exception as e:
            logging.error(f"{ups_name} 讀取現有檔案 {combined_path} 失敗：{e}")
            raise
    else:
        combined = df
 
    combined.to_csv(combined_path, index=False, encoding="utf-8-sig")  # 使用 UTF-8-SIG 確保 Excel 相容
    print(f"✅ {ups_name} 寫入 {combined_path}，總筆數：{len(combined)}")
 
except requests.RequestException as e:
    logging.error(f"{ups_name} 下載失敗：{e}")
    print(f"❌ {ups_name} 下載失敗：{e}")
except Exception as e:
    logging.error(f"{ups_name} 轉換失敗，路徑：{combined_path}，錯誤：{e}")
    print(f"⚠️ {ups_name} 轉換失敗：{e}")