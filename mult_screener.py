import yfinance as yf
import pandas as pd
import logging
import ast
import gc
import requests
import json
from itertools import islice
import time
import threading

# Configure logging
logger = logging.getLogger('yfinance')
logger.setLevel(logging.DEBUG)  # Adjust level as necessary
TELEGRAM_TOKEN='abc'
GROUP_CHAT_ID = '123'

# Define a custom log handler
class NotFoundLogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        super(NotFoundLogHandler, self).__init__(*args, **kwargs)
        self.invalid_tickers = []

    def emit(self, record):
        # Check if the log message indicates a not found error
        if '404' in record.getMessage() or 'Not Found' in record.getMessage():
            # Extract the ticker symbol from the message if possible
            message = record.getMessage()
            # Assuming the ticker symbol can be extracted from the message
            # This may need to be adjusted based on the actual log message format
            ticker_symbol = message.split(' ')[0]  # Placeholder extraction logic
            self.invalid_tickers.append(ticker_symbol)

# Instantiate and add the custom log handler to the yfinance logger
not_found_handler = NotFoundLogHandler()
logger.addHandler(not_found_handler)

# Initialize a list to store dictionaries for the top 30 ROIC companies
top_roic_companies = []
processed_tickers_count = 0
processed_tickers_lock = threading.Lock()

def send_telegram_message(chat_id, text):
    """Sends a message to the specified Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        'chat_id': chat_id,
        'text': text
    }
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

def process_ticker(ticker, company_name):
    global processed_tickers_count
    print(f"Processing ticker: {ticker.ticker} - {company_name}")
    try:
        market_cap = ticker.info.get("marketCap", 0)
        balance_sheet = ticker.balance_sheet

        # Ensure necessary data is available
        if market_cap <= 0 or 'Invested Capital' not in balance_sheet.index:
            return

        # Extract key balance sheet items
        invested_capital_current = balance_sheet.loc['Invested Capital'].iloc[0]
        total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0]
        total_debt = balance_sheet.loc['Total Debt'].iloc[0]
        # Use Preferred Stock if available; otherwise default to 0
        preferred_equity = balance_sheet.loc['Preferred Stock'].iloc[0] if 'Preferred Stock' in balance_sheet.index else 0

        # Calculate the Faustmann ratio (market cap relative to net worth)
        denominator = invested_capital_current + total_cash - total_debt - preferred_equity
        if denominator <= 0:
            return
        faustmann_ratio = round(market_cap / denominator, 3)
        if faustmann_ratio > 10 or faustmann_ratio is None:
            return

        # Fetch financials and calculate ROIC (Return on Invested Capital)
        ebit_series = ticker.financials.loc["EBIT"]
        invested_capital_series = balance_sheet.loc['Invested Capital']
        # Compute the mean ROIC; adjust this calculation if necessary
        roic = round((ebit_series / invested_capital_series).mean(), 3)

        # --- NEW FILTERS BASED ON The Dao of Capital IDEAS ---
        # 1. Filter for ROIC between 20% and 150%
        if roic < 0.20 or roic > 1.50:
            return

        # 2. Filter for low debt: Check if the debt-to-invested-capital ratio is less than 50%
        if invested_capital_current != 0:
            debt_ratio = total_debt / invested_capital_current
            if debt_ratio > 0.50:
                return
        else:
            return
        
        if pd.isna(faustmann_ratio) or pd.isna(roic) or pd.isna(debt_ratio):
            return
        
        # If all filters pass, add the company to the top_roic_companies list.
        company_data = {
            "Ticker": ticker.ticker,
            "Company": company_name,
            "Faustmann_Ratio": faustmann_ratio,
            "ROIC": roic,
            "Debt_Ratio": round(debt_ratio, 3)
        }
        if len(top_roic_companies) < 30:
            top_roic_companies.append(company_data)
        else:
            # If list is full, check if this company has a higher ROIC than the current lowest.
            min_roic_company = min(top_roic_companies, key=lambda x: x['ROIC'])
            if roic > min_roic_company['ROIC']:
                top_roic_companies.remove(min_roic_company)
                top_roic_companies.append(company_data)

        # If we have 30 companies, sort them by Faustmann ratio (lowest first),
        # then select the top 10 to send an update via Telegram.
        if len(top_roic_companies) == 30:
            top_roic_companies.sort(key=lambda x: x['Faustmann_Ratio'])
            top_10_faustmann = top_roic_companies[:10]
            message = "\n".join([
                f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}, Debt Ratio: {item['Debt_Ratio']}"
                for item in top_10_faustmann
            ])
            # send_telegram_message(GROUP_CHAT_ID, message)
        
    except requests.exceptions.HTTPError as e:
        if e.status_code == 404:
            pass
    except requests.exceptions.RequestException as err:
        pass
    except KeyError:
        pass
    except IndexError:
        pass
    except Exception as e:
        pass

    with processed_tickers_lock:
        processed_tickers_count += 1

    gc.collect()

# Function to parse a large dictionary file incrementally
def parse_large_dict(file_path):
    """ Generator function to parse a large dictionary file incrementally. """
    with open(file_path, 'r') as file:
        reading = False
        buffer = ''
        for line in file:
            if '{' in line:
                reading = True
            if reading:
                buffer += line.strip()
                if '},' in buffer or '}' in buffer:
                    # Handle the completion of a dictionary entry
                    if buffer.endswith(','):
                        buffer = buffer[:-1]  # remove trailing comma for last element
                    # Process buffer as a complete dictionary entry
                    try:
                        # Temporarily wrap buffer to make it a valid dict format if needed
                        data = eval(f"dict({buffer})")
                        for key, value in data.items():
                            yield key, value
                    except SyntaxError as e:
                        print(f"Error parsing buffer: {e}")
                    # Reset buffer after processing
                    buffer = ''
            if '}' in line:
                reading = False

# Function to get the last processed ticker
def get_last_processed_symbol(file_path):
    try:
        with open(file_path, 'r') as file:
            last_processed_symbol = file.read().strip()
        return last_processed_symbol
    except FileNotFoundError:
        return None

# Function to save the last processed ticker
def save_last_processed_symbol(symbol, file_path):
    with open(file_path, 'w') as file:
        file.write(symbol)

# Function to process a batch of tickers
def process_batch(tickers, companies):
    print(f"Processing batch: {tickers}")
    tickers_string = ' '.join(tickers)
    tickers_obj = yf.Tickers(tickers_string)

    # Fetch data for all tickers in the batch
    for ticker_symbol, company_name in zip(tickers, companies):
        ticker = tickers_obj.tickers[ticker_symbol]
        process_ticker(ticker, company_name)

# Function to get batches of tickers
def batch_generator(iterable, n=10):
    """Yield successive n-sized batches from iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

# Function to send hourly updates
def send_hourly_updates(total_tickers):
    while True:
        time.sleep(3600)  # Wait for 1 hour
        with processed_tickers_lock:
            processed = processed_tickers_count
        remaining = total_tickers - processed
        message = f"Processed: {processed} stocks\nRemaining: {remaining} stocks"
        try:
            # send_telegram_message(GROUP_CHAT_ID, message)
            print(f"Sent update: {message}")
        except Exception as e:
            print(f"Error sending update: {e}")

# Main logic to parse and process tickers
def main():
    last_processed_symbol = get_last_processed_symbol('last_processed.txt')
    start_processing = False if last_processed_symbol else True
    ticker_dict = dict(parse_large_dict('ticker_list_yf.txt'))

    if not start_processing:
        tickers = list(ticker_dict.keys())
        start_index = tickers.index(last_processed_symbol) + 1
    else:
        start_index = 0

    load_top_roic_companies('top_roic_companies.json')

    # Start the hourly update thread
    total_tickers = len(ticker_dict)
    update_thread = threading.Thread(target=send_hourly_updates, args=(total_tickers,), daemon=True)
    update_thread.start()

    for batch in batch_generator(list(ticker_dict.items())[start_index:], n=50):
        tickers, companies = zip(*batch)
        process_batch(tickers, companies)
        save_last_processed_symbol(tickers[-1], 'last_processed.txt')
        save_top_roic_companies('top_roic_companies.json')

# Call the main function to run the script
if __name__ == "__main__":
    main()
