import yfinance as yf
import pandas as pd
import logging
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
import time
import threading
import gc

# Configure logging
logger = logging.getLogger('yfinance')
logger.setLevel(logging.DEBUG)

TELEGRAM_TOKEN = 'abc'
GROUP_CHAT_ID = '123'

# Custom log handler for invalid tickers
class NotFoundLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.invalid_tickers = []

    def emit(self, record):
        if '404' in record.getMessage() or 'Not Found' in record.getMessage():
            ticker_symbol = record.getMessage().split(' ')[0]
            self.invalid_tickers.append(ticker_symbol)

not_found_handler = NotFoundLogHandler()
logger.addHandler(not_found_handler)

# Global variables
top_roic_companies = []
processed_tickers_count = 0
processed_tickers_lock = threading.Lock()
request_semaphore = threading.Semaphore(30)  # Limit to 30 requests/minute

def send_telegram_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': chat_id, 'text': text}
    response = requests.post(url, data=data)
    return response.json()

def save_top_roic_companies(file_path):
    with open(file_path, 'w') as file:
        json.dump(top_roic_companies, file)

def load_top_roic_companies(file_path):
    global top_roic_companies
    try:
        with open(file_path, 'r') as file:
            top_roic_companies = json.load(file)
    except FileNotFoundError:
        top_roic_companies = []

def process_ticker(ticker_symbol, company_name):
    global processed_tickers_count
    print(f"Processing ticker: {ticker_symbol} - {company_name}")
    
    with request_semaphore:
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            balance_sheet = ticker.balance_sheet
            financials = ticker.financials

            market_cap = info.get("marketCap", 0)
            if market_cap <= 0 or 'Invested Capital' not in balance_sheet.index:
                return

            invested_capital = balance_sheet.loc['Invested Capital'].iloc[0]
            total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0]
            total_debt = balance_sheet.loc['Total Debt'].iloc[0]
            preferred_equity = balance_sheet.loc.get('Preferred Stock', pd.Series([0])).iloc[0]

            denominator = invested_capital + total_cash - total_debt - preferred_equity
            if denominator <= 0:
                return
            faustmann_ratio = round(market_cap / denominator, 3)
            if faustmann_ratio > 3 or pd.isna(faustmann_ratio):
                return

            ebit = financials.loc["EBIT"].mean()
            roic = round(ebit / invested_capital, 3)
            if roic < 0.20 or roic > 1.50 or pd.isna(roic):
                return

            debt_ratio = total_debt / invested_capital if invested_capital != 0 else float('inf')
            if debt_ratio > 0.30 or pd.isna(debt_ratio):
                return

            company_data = {
                "Ticker": ticker_symbol,
                "Company": company_name,
                "Faustmann_Ratio": faustmann_ratio,
                "ROIC": roic,
                "Debt_Ratio": round(debt_ratio, 3)
            }
            with processed_tickers_lock:
                if len(top_roic_companies) < 30:
                    top_roic_companies.append(company_data)
                else:
                    min_roic_company = min(top_roic_companies, key=lambda x: x['ROIC'])
                    if roic > min_roic_company['ROIC']:
                        top_roic_companies.remove(min_roic_company)
                        top_roic_companies.append(company_data)

        except Exception:
            pass

    with processed_tickers_lock:
        processed_tickers_count += 1

def parse_large_dict(file_path):
    """Generator to parse a large Python dict-like file incrementally."""
    with open(file_path, 'r') as file:
        buffer = ''
        reading = False
        for line in file:
            if '{' in line:
                reading = True
            if reading:
                buffer += line.strip()
                if '},' in buffer or '}' in buffer:
                    if buffer.endswith(','):
                        buffer = buffer[:-1]
                    try:
                        data = eval(f"dict({buffer})")  # Safely evaluate as dict
                        for key, value in data.items():
                            yield key, value
                    except Exception as e:
                        print(f"Error parsing buffer: {e}")
                    buffer = ''
            if '}' in line:
                reading = False

def get_last_processed_symbol(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def save_last_processed_symbol(symbol, file_path):
    with open(file_path, 'w') as file:
        file.write(symbol)

def process_batch(batch):
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda x: process_ticker(*x), batch)

def batch_generator(iterable, n=50):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

def send_hourly_updates(total_tickers):
    while True:
        time.sleep(3600)
        with processed_tickers_lock:
            processed = processed_tickers_count
        remaining = total_tickers - processed
        message = f"Processed: {processed} stocks\nRemaining: {remaining} stocks"
        print(f"Sent update: {message}")

def main():
    last_processed_symbol = get_last_processed_symbol('last_processed.txt')
    ticker_dict = dict(parse_large_dict('ticker_list_yf.txt'))
    start_index = 0 if not last_processed_symbol else list(ticker_dict.keys()).index(last_processed_symbol) + 1

    load_top_roic_companies('top_roic_companies.json')
    total_tickers = len(ticker_dict)

    update_thread = threading.Thread(target=send_hourly_updates, args=(total_tickers,), daemon=True)
    update_thread.start()

    for batch in batch_generator(list(ticker_dict.items())[start_index:]):
        process_batch(batch)
        save_last_processed_symbol(batch[-1][0], 'last_processed.txt')
        save_top_roic_companies('top_roic_companies.json')
        time.sleep(2)  # Rate limiting

if __name__ == "__main__":
    main()