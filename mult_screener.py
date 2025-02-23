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
import queue  # Import the queue module for thread-safe communication

# Configure logging
logger = logging.getLogger('yfinance')
logger.setLevel(logging.DEBUG)  # Adjust level as necessary
TELEGRAM_TOKEN = 'abc'
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

# Use a queue to pass results from ticker processing threads
results_queue = queue.Queue()


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

def calculate_roic_data(ticker):
    """
    Fetches financial data and calculates ROIC related metrics.
    Returns a dictionary containing these metrics or None if data is insufficient.
    """
    try:
        market_cap = ticker.info.get("marketCap", 0)
        balance_sheet = ticker.balance_sheet

        # Ensure necessary data is available
        if market_cap <= 0 or 'Invested Capital' not in balance_sheet.index:
            return None

        # Extract key balance sheet items
        invested_capital_current = balance_sheet.loc['Invested Capital'].iloc[0]
        total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0]
        total_debt = balance_sheet.loc['Total Debt'].iloc[0]
        # Use Preferred Stock if available; otherwise default to 0
        preferred_equity = balance_sheet.loc['Preferred Stock'].iloc[0] if 'Preferred Stock' in balance_sheet.index else 0

        # Calculate the Faustmann ratio (market cap relative to net worth)
        denominator = invested_capital_current + total_cash - total_debt - preferred_equity
        if denominator <= 0:
            return None
        faustmann_ratio = round(market_cap / denominator, 3)

        if faustmann_ratio > 3: #Original: if faustmann_ratio > 3 or faustmann_ratio is None:
            return None

        # Fetch financials and calculate ROIC (Return on Invested Capital)
        ebit_series = ticker.financials.loc["EBIT"]
        invested_capital_series = balance_sheet.loc['Invested Capital']
        # Compute the mean ROIC; adjust this calculation if necessary
        roic = round((ebit_series / invested_capital_series).mean(), 3)


        # 2. Filter for low debt: Check if the debt-to-invested-capital ratio is less than 30%
        if invested_capital_current != 0:
            debt_ratio = total_debt / invested_capital_current
            if debt_ratio > 0.30:
                return None
        else:
            return None

        if pd.isna(faustmann_ratio) or pd.isna(roic) or pd.isna(debt_ratio):
            return None

        return {
            "Faustmann_Ratio": faustmann_ratio,
            "ROIC": roic,
            "Debt_Ratio": round(debt_ratio, 3)
        }

    except Exception: #Broad exception handling in data fetching to prevent thread crashes. More specific exceptions can be added as needed.
        return None


def process_ticker(ticker_symbol, company_name):
    global processed_tickers_count, not_found_handler

    max_retries = 3
    retry_delay = 5  # seconds

    for retry_attempt in range(max_retries):
        try:
            print(f"Processing ticker: {ticker_symbol} - {company_name} (Attempt {retry_attempt + 1})")

            ticker = yf.Ticker(ticker_symbol) # Create yf.Ticker object here

            # Clear invalid tickers list before processing each ticker
            not_found_handler.invalid_tickers = []

            roic_data = calculate_roic_data(ticker)

            if roic_data:
                # --- NEW FILTERS BASED ON The Dao of Capital IDEAS ---
                # 1. Filter for ROIC between 20% and 150% (Reduced lower bound to 20%)
                if roic_data["ROIC"] < 0.20 or roic_data["ROIC"] > 1.50:
                    return  # Exit if ROIC is outside the desired range


                # If all filters pass, prepare company data for results queue.
                company_data = {
                    "Ticker": ticker_symbol,
                    "Company": company_name,
                    "Faustmann_Ratio": roic_data["Faustmann_Ratio"],
                    "ROIC": roic_data["ROIC"],
                    "Debt_Ratio": roic_data["Debt_Ratio"]
                }
                results_queue.put(company_data) # Put valid company data into queue

            break  # Break out of retry loop on success (even if no ROIC data is returned, to avoid infinite retries on valid symbols with insufficient data)


        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"Ticker {ticker_symbol} not found (404).")
                break # Break retry loop for 404 errors - symbol does not exist.
            else:
                print(f"HTTP Error for {ticker_symbol}: {e}. Retry in {retry_delay} seconds...")
                time.sleep(retry_delay)
        except requests.exceptions.RequestException as e:
            print(f"Request Exception for {ticker_symbol}: {e}. Retry in {retry_delay} seconds...")
            time.sleep(retry_delay)
        except KeyError as e: # Handle cases where expected keys are missing in the data
            print(f"KeyError for {ticker_symbol}: {e}. Data may be missing or malformed. Skipping.")
            break # Skip ticker on KeyError, likely data is just not available, retrying won't help.
        except IndexError:
            print(f"IndexError for {ticker_symbol}. Data structure may be unexpected. Skipping.")
            break # Skip on index error - data structure issue, retry likely won't help
        except Exception as e: # Catch-all for other exceptions to prevent thread crash
            print(f"Unexpected error processing {ticker_symbol}: {e}. Retry in {retry_delay} seconds...")
            time.sleep(retry_delay)
        finally:
            with processed_tickers_lock:
                processed_tickers_count += 1
            gc.collect()
    else: # else block of for-loop, executes if no break occurred - all retries failed
        print(f"Failed to process {ticker_symbol} after {max_retries} retries. Skipping.")


def update_top_roic_companies():
    """
    Consumes results from the queue and updates the top_roic_companies list.
    """
    global top_roic_companies
    while not results_queue.empty():
        company_data = results_queue.get()
        if len(top_roic_companies) < 30:
            top_roic_companies.append(company_data)
        else:
            min_roic_company = min(top_roic_companies, key=lambda x: x['ROIC'])
            if company_data['ROIC'] > min_roic_company['ROIC']:
                top_roic_companies.remove(min_roic_company)
                top_roic_companies.append(company_data)

    if len(top_roic_companies) >= 30: # Sort and send Telegram message only after collecting enough companies.
        top_roic_companies.sort(key=lambda x: x['Faustmann_Ratio'])
        top_10_faustmann = top_roic_companies[:10]
        message = "\n".join([
            f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}, Debt Ratio: {item['Debt_Ratio']}"
            for item in top_10_faustmann
        ])
        # send_telegram_message(GROUP_CHAT_ID, message) # Uncomment to enable Telegram messages
        print("Top 10 Faustmann Ratio Companies (Telegram Message - Disabled):\n" + message)



# Function to process a batch of tickers using threads
def process_batch(batch):
    threads = []
    for ticker_symbol, company_name in batch:
        thread = threading.Thread(target=process_ticker, args=(ticker_symbol, company_name))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()  # Wait for all threads in the batch to complete
    
    # Make sure we process all results before saving
    while not results_queue.empty():
        update_top_roic_companies()  # Process all results in queue
    save_top_roic_companies('top_roic_companies.json')  # Save after processing all results


# Function to parse a large dictionary file incrementally (no changes needed)
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

# Function to get the last processed ticker (no changes needed)
def get_last_processed_symbol(file_path):
    try:
        with open(file_path, 'r') as file:
            last_processed_symbol = file.read().strip()
        return last_processed_symbol
    except FileNotFoundError:
        return None

# Function to save the last processed ticker (no changes needed)
def save_last_processed_symbol(symbol, file_path):
    with open(file_path, 'w') as file:
        file.write(symbol)


# Function to get batches of tickers (no changes needed)
def batch_generator(iterable, n=50): # Increased batch size to 50 for potentially better throughput
    """Yield successive n-sized batches from iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

# Function to send hourly updates (no changes needed)
def send_hourly_updates(total_tickers):
    while True:
        time.sleep(3600)  # Wait for 1 hour
        with processed_tickers_lock:
            processed = processed_tickers_count
        remaining = total_tickers - processed
        message = f"Processed: {processed} stocks\nRemaining: {remaining} stocks"
        try:
            # send_telegram_message(GROUP_CHAT_ID, message) # Uncomment to enable Telegram messages
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

    # Start the hourly update thread (no changes needed)
    total_tickers = len(ticker_dict)
    update_thread = threading.Thread(target=send_hourly_updates, args=(total_tickers,), daemon=True)
    update_thread.start()

    for batch in batch_generator(list(ticker_dict.items())[start_index:], n=50): # Process items directly, batch_generator yields lists of (ticker, company_name) tuples now.
        process_batch(batch) # Pass batch directly to process_batch which now handles threading within the batch.
        if batch: # Ensure batch is not empty before saving last processed symbol
            save_last_processed_symbol(batch[-1][0], 'last_processed.txt') # Save last ticker of the batch
        save_top_roic_companies('top_roic_companies.json')

    print("Script finished processing all tickers.")
    if top_roic_companies:
        top_roic_companies.sort(key=lambda x: x['Faustmann_Ratio'])
        top_10_faustmann = top_roic_companies[:10]
        message = "\n".join([
            f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}, Debt Ratio: {item['Debt_Ratio']}"
            for item in top_10_faustmann
        ])
        print("Top 10 ROIC Companies (Final Result):\n" + message)
        # send_telegram_message(GROUP_CHAT_ID, message) # Optionally send final results to Telegram

# Call the main function to run the script
if __name__ == "__main__":
    main()