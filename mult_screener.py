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

# Initialize the Telegram Bot with your token
TELEGRAM_TOKEN = '6717990254:AAGFOqjtHJ7gRD0enLdQvCkIFvJTtFOzYM'
GROUP_CHAT_ID = '-4220170140'

# Configure logging
logger = logging.getLogger('yfinance')
logger.setLevel(logging.DEBUG)  # Adjust level as necessary

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

        # Check if necessary data is available
        if market_cap > 0 and 'Invested Capital' in balance_sheet.index and 'Cash And Cash Equivalents' in balance_sheet.index and 'Total Debt' in balance_sheet.index:
            invested_capital_current = balance_sheet.loc['Invested Capital'].iloc[0]
            total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0]
            total_debt = balance_sheet.loc['Total Debt'].iloc[0]
            
            # Check if Preferred Stock is available, otherwise set it to 0
            preferred_equity = balance_sheet.loc['Preferred Stock'].iloc[0] if 'Preferred Stock' in balance_sheet.index else 0

            # Calculate the Faustmann ratio
            faustmann_ratio = round(market_cap / (invested_capital_current + total_cash - total_debt - preferred_equity), 3)

            if faustmann_ratio > 0:
                ebit = ticker.financials.loc["EBIT"]
                invested_capital = balance_sheet.loc['Invested Capital']
                roic = round((ebit / invested_capital).mean(), 3)

                # Add to top ROIC companies list if not full, else replace the lowest ROIC if the new one is higher
                if len(top_roic_companies) < 30:
                    top_roic_companies.append({"Ticker": ticker.ticker, "Company": company_name, "Faustmann_Ratio": faustmann_ratio, "ROIC": roic})
                else:
                    min_roic = min(top_roic_companies, key=lambda x: x['ROIC'])
                    if roic > min_roic['ROIC']:
                        top_roic_companies.remove(min_roic)
                        top_roic_companies.append({"Ticker": ticker.ticker, "Company": company_name, "Faustmann_Ratio": faustmann_ratio, "ROIC": roic})

                # Sort and get the top 10 by lowest Faustmann ratio
                if len(top_roic_companies) == 30:
                    top_roic_companies.sort(key=lambda x: x['Faustmann_Ratio'])
                    top_10_faustmann = top_roic_companies[:10]
                    message = "\n".join([f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}" for item in top_10_faustmann])
                    send_telegram_message(GROUP_CHAT_ID, message)
        
    except requests.exceptions.HTTPError as e:
        if e.status_code == 404:
            pass

    except requests.exceptions.RequestException as err:
        pass
    
    except KeyError:
        # Handle specific missing data errors quietly or log them
        pass

    except IndexError:
        # Handle cases where .iloc[] fails due to missing data
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
        send_telegram_message(GROUP_CHAT_ID, message)

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
