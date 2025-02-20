import yfinance as yf
import pandas as pd
import logging
import requests
import json
from itertools import islice
import time
import threading
import concurrent.futures
import gc

# Configure logging (simplified)
logger = logging.getLogger('yfinance')
logger.setLevel(logging.INFO)
TELEGRAM_TOKEN = 'abc'
GROUP_CHAT_ID = '123'

# Initialize shared data structures (thread-safe)
top_roic_companies = []
processed_tickers_count = 0
processed_tickers_lock = threading.Lock()
top_roic_companies_lock = threading.Lock()  # Lock for modifying the top companies list

def send_telegram_message(chat_id, text):
    """Sends a message to the specified Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': chat_id, 'text': text}
    try:
        response = requests.post(url, data=data, timeout=10)  # Added timeout
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error sending Telegram message: {e}")
        return None

def save_top_roic_companies(file_path):
    with top_roic_companies_lock:
        try:
            with open(file_path, 'w') as file:
                json.dump(top_roic_companies, file)
        except Exception as e:
            print(f"Error saving top ROIC companies: {e}")


def load_top_roic_companies(file_path):
    global top_roic_companies
    try:
        with open(file_path, 'r') as file:
            with top_roic_companies_lock:
                top_roic_companies = json.load(file)
    except FileNotFoundError:
        top_roic_companies = []
    except json.JSONDecodeError:
        print("Error: top_roic_companies.json is corrupted.  Starting with an empty list.")
        top_roic_companies = []
    except Exception as e:
        print(f"An unexpected error occurred loading data: {e}")
        top_roic_companies = []


def process_ticker(ticker_symbol, company_name, session):
    """Processes a single ticker with retries and exponential backoff."""
    global processed_tickers_count
    global top_roic_companies

    max_retries = 500
    base_delay = 180  # Initial delay in seconds
    retries = 0

    while retries < max_retries:
        try:
            # Use a session for the individual ticker (optional, but good practice)
            ticker = yf.Ticker(ticker_symbol, session=session)

            # MINIMIZE MODULES: Only fetch what you absolutely need
            info = ticker.get_info(proxy=None)  # Fetch info once
            market_cap = info.get("marketCap", 0)

            # Prefetch in batches (important optimization)
            balance_sheet = ticker.balance_sheet
            financials = ticker.financials


            # --- DATA VALIDATION AND EARLY EXIT ---
            if market_cap <= 0 or balance_sheet.empty or financials.empty or 'Invested Capital' not in balance_sheet.index:
                return

            # Extract balance sheet data with safer .get() and error handling
            invested_capital_current = balance_sheet.loc['Invested Capital'].iloc[0] if 'Invested Capital' in balance_sheet.index else None
            total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0] if 'Cash And Cash Equivalents' in balance_sheet.index else None
            total_debt = balance_sheet.loc['Total Debt'].iloc[0] if 'Total Debt' in balance_sheet.index else None
            preferred_equity = balance_sheet.loc['Preferred Stock'].iloc[0] if 'Preferred Stock' in balance_sheet.index else 0

            if invested_capital_current is None or total_cash is None or total_debt is None:
                return  # Exit early if crucial data is missing

            # Calculate Faustmann ratio
            denominator = invested_capital_current + total_cash - total_debt - preferred_equity
            if denominator <= 0:
                return
            faustmann_ratio = round(market_cap / denominator, 3)
            if faustmann_ratio > 10 or pd.isna(faustmann_ratio):
                return

            # --- ROIC CALCULATION ---
            ebit_series = financials.loc["EBIT"] if "EBIT" in financials.index else None
            invested_capital_series = balance_sheet.loc['Invested Capital'] if 'Invested Capital' in balance_sheet.index else None

            if ebit_series is None or invested_capital_series is None or invested_capital_series.empty:
                return
            try:
               roic = round((ebit_series / invested_capital_series).mean(), 3)
            except TypeError:
                return


            # --- FILTERS (Optimized) ---
            if not (0.20 <= roic <= 1.50):  # Combined ROIC check
                return

            debt_ratio = total_debt / invested_capital_current if invested_capital_current != 0 else None
            if debt_ratio is None or debt_ratio > 0.50:
                return

            if pd.isna(roic) or pd.isna(debt_ratio): #Removed faustman ratio from here, because that's checked earlier
                return

            # --- UPDATE TOP COMPANIES (Thread-safe) ---
            company_data = {
                "Ticker": ticker_symbol,
                "Company": company_name,
                "Faustmann_Ratio": faustmann_ratio,
                "ROIC": roic,
                "Debt_Ratio": round(debt_ratio, 3)
            }

            with top_roic_companies_lock:
                if len(top_roic_companies) < 30:
                    top_roic_companies.append(company_data)
                else:
                    min_roic_company = min(top_roic_companies, key=lambda x: x['ROIC'])
                    if roic > min_roic_company['ROIC']:
                        top_roic_companies.remove(min_roic_company)
                        top_roic_companies.append(company_data)
            return  # Exit on success

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = base_delay * (2 ** retries)  # Exponential backoff
                print(f"Rate limited ({ticker_symbol}). Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
            else:
                # Handle other HTTP errors (e.g., 404)
                #print(f"HTTP Error processing {ticker_symbol}: {e}") #Removed for cluttering reasons
                return  # Don't retry for non-429 errors

        except (requests.exceptions.RequestException, KeyError, IndexError, TypeError, ValueError) as e:
            # Catch specific exceptions for better error handling
            #print(f"Error processing {ticker_symbol}: {type(e).__name__} - {e}") #Removed for cluttering reasons
            return  # Don't retry on these errors.
        except Exception as e:
            print(f"Unexpected error processing {ticker_symbol}: {type(e).__name__} - {e}")
            return

    print(f"Max retries reached for {ticker_symbol}. Skipping.")


def process_batch(batch):
    """Processes a batch of tickers using a session and ThreadPoolExecutor."""
    tickers, companies = zip(*batch)

    with requests.Session() as session:  # Create a session for the batch
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:  # Reduce workers
            futures = [executor.submit(process_ticker, ticker, company, session) for ticker, company in zip(tickers, companies)]
            concurrent.futures.wait(futures)

    with processed_tickers_lock:
       global processed_tickers_count  # Ensure correct scope
       processed_tickers_count += len(batch)  # Update count *after* batch completion

    gc.collect()

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

def get_last_processed_symbol(file_path):
    """Gets the last processed ticker symbol from a file."""
    try:
        with open(file_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


def save_last_processed_symbol(symbol, file_path):
    """Saves the last processed ticker symbol to a file."""
    try:
        with open(file_path, 'w') as file:
            file.write(symbol)
    except Exception as e:
        print(f"Error saving last processed symbol: {e}")


def batch_generator(iterable, n=50):
    """Yield successive n-sized batches from iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

def send_hourly_updates(total_tickers):
    """Sends hourly updates to Telegram."""
    while True:
        time.sleep(3600)
        with processed_tickers_lock:
            processed = processed_tickers_count
        remaining = total_tickers - processed
        message = f"Processed: {processed} stocks\nRemaining: {remaining} stocks"
        send_telegram_message(GROUP_CHAT_ID, message)

def send_completion_message():
     with top_roic_companies_lock:
        if len(top_roic_companies) > 0:  # Ensure there's data to send
            top_roic_companies.sort(key=lambda x: x['Faustmann_Ratio'])
            top_10_faustmann = top_roic_companies[:10]  # Get the top 10
            message = "Top 10 Companies by Faustmann Ratio (after full scan):\n" + "\n".join([
            f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}, Debt Ratio: {item['Debt_Ratio']}"
            for item in top_10_faustmann
            ])

            send_telegram_message(GROUP_CHAT_ID, message)
        else:
             send_telegram_message(GROUP_CHAT_ID, "No companies met the criteria after the full scan.")

def main():
    """Main function to orchestrate the process."""
    last_processed_symbol = get_last_processed_symbol('last_processed.txt')
    ticker_dict = dict(parse_large_dict('ticker_list_yf.txt'))  # Load the entire dictionary

    # Determine the starting point based on the last processed symbol
    if last_processed_symbol:
        tickers = list(ticker_dict.keys())
        try:
            start_index = tickers.index(last_processed_symbol) + 1
        except ValueError:
            start_index = 0  # Start from the beginning if not found
    else:
        start_index = 0

    load_top_roic_companies('top_roic_companies.json')

    total_tickers = len(ticker_dict)
    update_thread = threading.Thread(target=send_hourly_updates, args=(total_tickers,), daemon=True)
    update_thread.start()

    # Use a larger batch size and process batches
    for batch in batch_generator(list(ticker_dict.items())[start_index:], n=50):  # reduced batch size
        process_batch(batch)
        if batch:  # Check if the batch is not empty
           save_last_processed_symbol(batch[-1][0], 'last_processed.txt')
        save_top_roic_companies('top_roic_companies.json')
        time.sleep(2) #Added an extra delay after each batch
    send_completion_message()

if __name__ == "__main__":
    main()