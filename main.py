import os
import sys
import json
import datetime
import pandas as pd
import yfinance as yf
from fredapi import Fred
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, PushMessageRequest, TextMessage

# --- 1. 環境變數讀取 ---
FRED_KEY = os.environ.get("FRED_API_KEY")
LINE_TOKEN = os.environ.get("LINE_TOKEN")
LINE_USER_ID = os.environ.get("LINE_USER_ID")
SHEET_KEY = os.environ.get("SHEET_KEY")
GCP_JSON = os.environ.get("GCP_CREDENTIALS_JSON") # 將整個 JSON 內容當字串讀入

# --- 2. 工具函式 ---
def get_today_str():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d")

def send_line(msg):
    try:
        configuration = Configuration(access_token=LINE_TOKEN)
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message(PushMessageRequest(to=LINE_USER_ID, messages=[TextMessage(text=msg)]))
    except Exception as e:
        print(f"LINE 發送失敗: {e}")

# --- 3. 核心邏輯 Class ---
class MungerRiskSystem:
    def __init__(self):
        self.data = {}
        self.score = 0
        self.level = 0
        self.reasons = []
        self.fred = Fred(api_key=FRED_KEY)

    def fetch_data(self):
        print("Fetching Data...")
        # A. FRED 總經數據 (取最新一筆)
        try:
            # T10Y3M: 10年-3個月公債利差
            self.data['yield_spread'] = self.fred.get_series('T10Y3M').iloc[-1]
            # NFCI: 金融壓力指數
            self.data['nfci'] = self.fred.get_series('NFCI').iloc[-1]
        except Exception as e:
            print(f"FRED Error: {e}")
            self.data['yield_spread'] = 0 # Default safe
            self.data['nfci'] = 0
            
        # B. Yahoo Finance 市場數據
        try:
            # 抓取 SPY, VIX, HYG, IEF
            tickers = yf.download(["SPY", "^VIX", "HYG", "IEF"], period="200d", progress=False)['Close']
            
            # 處理最新一筆資料
            last_idx = tickers.index[-1]
            self.data['us_date'] = last_idx.strftime("%Y-%m-%d")
            self.data['vix'] = tickers.loc[last_idx, "^VIX"]
            self.data['spy_close'] = tickers.loc[last_idx, "SPY"]
            self.data['spy_ma200'] = tickers["SPY"].rolling(200).mean().iloc[-1]
            
            # 債券流動性指標 (HYG/IEF)
            hyg_ief_ratio = tickers["HYG"] / tickers["IEF"]
            self.data['hyg_ief_curr'] = hyg_ief_ratio.iloc[-1]
            self.data['hyg_ief_ma60'] = hyg_ief_ratio.rolling(60).mean().iloc[-1]
            
        except Exception as e:
            print(f"Yahoo Finance Error: {e}")
            raise e # 資料源掛了直接報錯停止

    def calculate_risk(self):
        print("Calculating Risk...")
        # 引擎 A: 地基 (權重高)
        # 1. 殖利率倒掛
        if self.data['yield_spread'] < 0:
            self.score += 3
            self.reasons.append(f"🔴殖利率倒掛({self.data['yield_spread']:.2f})")
        
        # 2. 金融緊縮
        if self.data['nfci'] > 0.5:
            self.score += 2
            self.reasons.append(f"🔴資金緊縮(NFCI {self.data['nfci']:.2f})")
        elif self.data['nfci'] > 0:
            self.score += 1
            self.reasons.append(f"🟡資金微緊")
            
        # 3. 債市聰明錢 (HYG/IEF)
        if self.data['hyg_ief_curr'] < self.data['hyg_ief_ma60']:
            self.score += 2 # 改為 2 分
            self.reasons.append("🟡信用利差轉弱")

        # 引擎 B: 市場溫度
        # 1. VIX
        if self.data['vix'] > 30:
            self.score += 2
            self.reasons.append(f"🔴極度恐慌(VIX {self.data['vix']:.1f})")
        elif self.data['vix'] > 20:
            self.score += 1
            self.reasons.append(f"🟡避險情緒高")
            
        # 2. 趨勢
        if self.data['spy_close'] < self.data['spy_ma200']:
            self.score += 1
            self.reasons.append("🔴SPY跌破年線")

        # 判定等級
        if self.score == 0: self.level = 0
        elif self.score <= 2: self.level = 1
        elif self.score <= 4: self.level = 2
        else: self.level = 3

    def save_to_sheet(self):
        print("Saving to Google Sheets...")
        # 解析 GCP Credentials
        creds_dict = json.loads(GCP_JSON)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        sheet = client.open_by_key(SHEET_KEY)
        # 根據年份選擇分頁，若無則預設第一個
        try:
            worksheet = sheet.worksheet(f"Data_{get_today_str()[:4]}")
        except:
            worksheet = sheet.get_worksheet(0)
            
        # 讀取最後一筆檢查是否重複 (Idempotency)
        all_records = worksheet.get_all_values()
        if len(all_records) > 1:
            last_date = all_records[-1][0] # 假設 A 欄是日期
            if last_date == get_today_str():
                print("Today already executed. Skip.")
                return False # 重複執行

        # 準備寫入資料
        row = [
            get_today_str(),               # A: Execute Date
            self.data.get('us_date', ''),  # B: US Date
            self.level,                    # C: Level
            self.score,                    # D: Score
            ", ".join(self.reasons),       # E: Reasons
            json.dumps(self.data)          # F: Raw Data JSON
        ]
        
        # 如果是第一列，寫入 Header
        if len(all_records) == 0:
            worksheet.append_row(["Execute_Date", "US_Date", "Level", "Score", "Reasons", "Raw_Data"])
            
        worksheet.append_row(row)
        return True

    def notify(self, is_new_record):
        # 簡易通知邏輯：只有 Level >= 2 或 週五 才通知，避免干擾
        # 這裡示範每次執行都通知摘要
        if not is_new_record: return

        emoji_map = {0: "🟢", 1: "🟡", 2: "🟠", 3: "🔴"}
        emoji = emoji_map.get(self.level, "⚪")
        
        msg = f"【蒙格風險日報】\n{get_today_str()}\n"
        msg += f"風險等級: {emoji} Lv.{self.level} (分: {self.score})\n"
        msg += "----------------\n"
        if self.reasons:
            msg += "\n".join(self.reasons)
        else:
            msg += "市場地基穩固"
            
        if self.level >= 2:
            msg += "\n\n⚠️ 建議檢視曝險部位"
            
        send_line(msg)

# --- 4. 主程式進入點 ---
if __name__ == "__main__":
    try:
        system = MungerRiskSystem()
        system.fetch_data()
        system.calculate_risk()
        is_saved = system.save_to_sheet()
        system.notify(is_saved)
        print("執行成功")
    except Exception as e:
        error_msg = f"系統執行錯誤: {e}"
        print(error_msg)
        send_line(error_msg) # 錯誤也要通知
        sys.exit(1)
