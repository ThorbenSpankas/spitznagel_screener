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
import queue

# Configure logging
logger = logging.getLogger('yfinance')
logger.setLevel(logging.DEBUG)  # Adjust level as necessary
TELEGRAM_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'  # Replace with your token
GROUP_CHAT_ID = 'YOUR_CHAT_ID'  # Replace with your chat ID

# Define a custom log handler
class NotFoundLogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        super(NotFoundLogHandler, self).__init__(*args, **kwargs)
        self.invalid_tickers = []

    def emit(self, record):
        # Check if the log message indicates a not found error
        if '404' in record.getMessage() or 'Not Found' in record.getMessage():
            # Extract the ticker symbol
            message = record.getMessage()
            try:
                # More robust ticker extraction, handling different message formats
                ticker_symbol = message.split()[0].strip(':,."') # Remove common punctuation
            except:
                ticker_symbol = "UNKNOWN" # Avoid crash if splitting fails
            self.invalid_tickers.append(ticker_symbol)

# Instantiate and add the custom log handler to the yfinance logger
not_found_handler = NotFoundLogHandler()
logger.addHandler(not_found_handler)

# Initialize a list to store dictionaries for the top ROIC companies
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
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error sending Telegram message: {e}")
        return None


def save_top_roic_companies(file_path):
    try:
        with open(file_path, 'w') as file:
            json.dump(top_roic_companies, file, indent=4)  # Use indent for readability
    except Exception as e:
        print(f"Error saving top ROIC companies: {e}")

def load_top_roic_companies(file_path):
    global top_roic_companies
    try:
        with open(file_path, 'r') as file:
            top_roic_companies = json.load(file)
    except FileNotFoundError:
        top_roic_companies = []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {file_path}: {e}")
        top_roic_companies = []  # Reset to avoid using corrupted data
    except Exception as e:
        print(f"Error loading top ROIC companies: {e}")
        top_roic_companies = []


def calculate_roic_data(ticker):
    """
    Fetches financial data and calculates ROIC related metrics.
    Returns a dictionary containing these metrics or None if data is insufficient.
    """
    try:
        # --- Market Cap Check ---
        try:
            ticker_info = ticker.info  # Fetch info once
            market_cap = ticker_info.get("marketCap", 0)
            if market_cap is None or market_cap <= 0:
                return None
        except (AttributeError, TypeError, ValueError) as e:
            print(f"Market cap error for {ticker.ticker}: {e}")
            return None
        except Exception as e: # Catch json decode errors
            print(f"Unexpected error fetching ticker info for {ticker.ticker}: {e}")
            return None

        # --- Balance Sheet ---
        try:
            balance_sheet = ticker.balance_sheet
            if balance_sheet.empty:
                print(f"Empty balance sheet for {ticker.ticker}")
                return None
        except Exception as e:
            print(f"Balance sheet error for {ticker.ticker}: {e}")
            return None

        # --- Financials ---
        try:
            financials = ticker.financials
            if financials.empty:
                print(f"Empty financials for {ticker.ticker}")
                return None
        except Exception as e:
            print(f"Financials error for {ticker.ticker}: {e}")
            return None

        # --- Extract Key Balance Sheet Items ---
        try:
            # Check if 'Invested Capital' exists in balance sheet
            if 'Invested Capital' not in balance_sheet.index:
                # Try to calculate it from other data
                total_assets = balance_sheet.loc['Total Assets'].iloc[0] if 'Total Assets' in balance_sheet.index else None
                if total_assets is None or pd.isna(total_assets):
                    print(f"Missing Total Assets for {ticker.ticker}")
                    return None
                
                # Use Total Assets as a fallback for Invested Capital
                invested_capital_current = total_assets
            else:
                invested_capital_current = balance_sheet.loc['Invested Capital'].iloc[0]
            
            if pd.isna(invested_capital_current) or invested_capital_current <= 0:
                print(f"Invalid Invested Capital for {ticker.ticker}")
                return None

            # Get key financial items with safe fallbacks
            total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0] if 'Cash And Cash Equivalents' in balance_sheet.index else 0
            total_debt = balance_sheet.loc['Total Debt'].iloc[0] if 'Total Debt' in balance_sheet.index else 0
            preferred_equity = balance_sheet.loc['Preferred Stock'].iloc[0] if 'Preferred Stock' in balance_sheet.index else 0

            # Handle potential NaN values
            total_cash = 0 if pd.isna(total_cash) else total_cash
            total_debt = 0 if pd.isna(total_debt) else total_debt
            preferred_equity = 0 if pd.isna(preferred_equity) else preferred_equity

        except Exception as e:
            print(f"Error extracting balance sheet items for {ticker.ticker}: {e}")
            return None

        # --- ROIC Calculation ---
        try:
            # Check if EBIT exists
            if "EBIT" not in financials.index:
                print(f"Missing EBIT for {ticker.ticker}")
                return None
            
            ebit_series = financials.loc["EBIT"]
            
            # Create an array of appropriate invested capital values
            invested_capital_series = balance_sheet.loc['Invested Capital'] if 'Invested Capital' in balance_sheet.index else balance_sheet.loc['Total Assets']

            if len(ebit_series) == 0 or len(invested_capital_series) == 0:
                print(f"Empty EBIT or Invested Capital series for {ticker.ticker}")
                return None

            roic_values = []
            for i in range(min(len(ebit_series), len(invested_capital_series))):
                if not pd.isna(ebit_series.iloc[i]) and not pd.isna(invested_capital_series.iloc[i]) and invested_capital_series.iloc[i] > 0:
                    roic_values.append(ebit_series.iloc[i] / invested_capital_series.iloc[i])

            if not roic_values:
                print(f"No valid ROIC values calculated for {ticker.ticker}")
                return None

            roic = round(sum(roic_values) / len(roic_values), 3)

            if roic < 0.20 or roic > 1.50:
                print(f"ROIC outside valid range for {ticker.ticker}: {roic}")
                return None
        except Exception as e:
            print(f"ROIC calculation error for {ticker.ticker}: {e}")
            return None

        # --- Faustmann Ratio Calculation ---
        try:
            denominator = invested_capital_current + total_cash - total_debt - preferred_equity
            if denominator <= 0:
                print(f"Invalid Faustmann denominator for {ticker.ticker}")
                return None
            faustmann_ratio = round(market_cap / denominator, 3)

            # if faustmann_ratio > 20:
            #     print(f"Faustmann ratio too high for {ticker.ticker}: {faustmann_ratio}")
            #     return None
        except Exception as e:
            print(f"Faustmann ratio calculation error for {ticker.ticker}: {e}")
            return None

        # --- Debt Ratio Calculation ---
        try:
            debt_ratio = round(total_debt / invested_capital_current, 3)
        except Exception as e:
            print(f"Debt ratio calculation error for {ticker.ticker}: {e}")
            debt_ratio = -100  # Default to 0 if calculation fails

        if pd.isna(faustmann_ratio) or pd.isna(roic) or pd.isna(debt_ratio):
            print(f"NaN values in final calculations for {ticker.ticker}")
            return None

        return {
            "Faustmann_Ratio": faustmann_ratio,
            "ROIC": roic,
            "Debt_Ratio": debt_ratio
        }

    except Exception as e:
        print(f"Error in calculate_roic_data for {ticker.ticker}: {str(e)}")
        return None



def process_ticker(ticker_symbol, company_name):
    global processed_tickers_count, not_found_handler

    max_retries = 3
    retry_delay = 5  # seconds

    for retry_attempt in range(max_retries):
        try:
            print(f"Processing ticker: {ticker_symbol} - {company_name} (Attempt {retry_attempt + 1})")

            # Create yf.Ticker object here
            ticker = yf.Ticker(ticker_symbol)

            # Clear invalid tickers list
            not_found_handler.invalid_tickers = []

            # Add rate limiting
            if retry_attempt > 0:
                time.sleep(2)

            roic_data = calculate_roic_data(ticker)

            if roic_data:
                company_data = {
                    "Ticker": ticker_symbol,
                    "Company": company_name,
                    "Faustmann_Ratio": roic_data["Faustmann_Ratio"],
                    "ROIC": roic_data["ROIC"],
                    "Debt_Ratio": roic_data["Debt_Ratio"]
                }
                results_queue.put(company_data)
                print(f"✓ {ticker_symbol} added with ROIC: {roic_data['ROIC']}, Faustmann: {roic_data['Faustmann_Ratio']}")
            else:
                print(f"× {ticker_symbol} - Insufficient data for ROIC calculation")

            break  # Success or no data available

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"Ticker {ticker_symbol} not found (404).")
                break  # Break on 404
            else:
                print(f"HTTP Error for {ticker_symbol}: {e}. Retry in {retry_delay} seconds...")
                time.sleep(retry_delay)
        except requests.exceptions.RequestException as e:
            print(f"Request Exception for {ticker_symbol}: {e}. Retry in {retry_delay} seconds...")
            time.sleep(retry_delay)
        except KeyError as e:
            print(f"KeyError for {ticker_symbol}: {e}. Data may be missing. Skipping.")
            break
        except IndexError as e:
            print(f"IndexError for {ticker_symbol}: {e}. Data structure issue. Skipping.")
            break
        except json.JSONDecodeError as e:  # Handle JSON decode errors specifically
            print(f"JSON Decode Error for {ticker_symbol}: {e}. Data may be corrupted. Skipping.")
            break
        except Exception as e:
            print(f"Unexpected error processing {ticker_symbol}: {e}. Retry in {retry_delay} seconds...")
            time.sleep(retry_delay)
        finally:
            with processed_tickers_lock:
                processed_tickers_count += 1
            gc.collect()
    else:
        print(f"Failed to process {ticker_symbol} after {max_retries} retries.")


def update_top_roic_companies():
    """
    Consumes results from the queue, updates and sorts the top_roic_companies list.
    """
    global top_roic_companies

    new_companies = []
    while not results_queue.empty():
        try:
            company_data = results_queue.get(block=False)  # Non-blocking get
            new_companies.append(company_data)
        except queue.Empty:
            break  # No more items in queue
        except Exception as e:
            print(f"Error getting item from queue: {e}")

    if new_companies:
        top_roic_companies.extend(new_companies)
        print(f"Added {len(new_companies)} new companies to tracking list")

        if len(top_roic_companies) >= 30:
            # Sort by ROIC first (descending)
            top_roic_companies.sort(key=lambda x: x['ROIC'], reverse=True)
            # Limit to top 100 by ROIC
            top_roic_companies = top_roic_companies[:100]
            # Then sort by Faustmann (ascending)
            top_roic_companies.sort(key=lambda x: x['Faustmann_Ratio'])
            # Limit to top 30 by Faustmann
            top_roic_companies = top_roic_companies[:30]

            top_10_faustmann = top_roic_companies[:10]

            message = "\n".join([
                f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}, Debt Ratio: {item['Debt_Ratio']}"
                for item in top_10_faustmann
            ])
            # send_telegram_message(GROUP_CHAT_ID, message)  # Uncomment for Telegram
            print("\nTop 10 Companies (Current):")
            print(message)
            print("\n")


def process_batch(batch):
    threads = []
    for ticker_symbol, company_name in batch:
        thread = threading.Thread(target=process_ticker, args=(ticker_symbol, company_name))
        threads.append(thread)
        thread.start()
        time.sleep(0.2)  # Rate limiting

    for thread in threads:
        thread.join()

    update_top_roic_companies()
    save_top_roic_companies('top_roic_companies.json')


def parse_large_dict(file_path):
    """ Generator function to parse a large dictionary file incrementally. """
    try:
        with open(file_path, 'r') as file:
            reading = False
            buffer = ''
            for line in file:
                if '{' in line and not reading:
                    reading = True
                    buffer = line.strip()
                elif reading:
                    buffer += line.strip()
                    
                    if ('},' in buffer or '}' in buffer) and buffer.count('{') == buffer.count('}'):
                        if buffer.endswith(','):
                            buffer = buffer[:-1]
                        try:
                            # Parse the dictionary entry
                            if buffer.startswith('{') and buffer.endswith('}'):
                                data = ast.literal_eval(buffer)
                                for key, value in data.items():
                                    yield key, value
                            else:
                                # Try to handle it as a dict entry
                                data = ast.literal_eval(f"dict({buffer})")
                                for key, value in data.items():
                                    yield key, value
                        except (SyntaxError, ValueError) as e:
                            print(f"Error parsing buffer: {e}")
                            print(f"Problematic buffer: {buffer}")
                        buffer = ''
                        reading = False
    except Exception as e:
        print(f"Error reading ticker file: {e}")


def get_last_processed_symbol(file_path):
    try:
        with open(file_path, 'r') as file:
            last_processed_symbol = file.read().strip()
        return last_processed_symbol
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"Error reading last processed symbol: {e}")
        return None

def save_last_processed_symbol(symbol, file_path):
    try:
        with open(file_path, 'w') as file:
            file.write(symbol)
    except Exception as e:
        print(f"Error saving last processed symbol: {e}")

def batch_generator(iterable, n=10):
    """Yield successive n-sized batches from iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

def send_hourly_updates(total_tickers):
    while True:
        time.sleep(3600)  # Wait for 1 hour
        with processed_tickers_lock:
            processed = processed_tickers_count
        remaining = total_tickers - processed
        message = f"Processed: {processed} stocks\nRemaining: {remaining} stocks"
        try:
            # send_telegram_message(GROUP_CHAT_ID, message) # Uncomment for Telegram
            print(f"Sent update: {message}")
        except Exception as e:
            print(f"Error sending update: {e}")

# Main logic to parse and process tickers
def main():
    try:
        last_processed_symbol = get_last_processed_symbol('last_processed.txt')
        start_processing = False if last_processed_symbol else True
        
        print("Loading ticker dictionary...")
        ticker_dict = {}
        ticker_count = 0
        
        # Use the generator to load tickers
        for symbol, name in parse_large_dict('cleaned_tickers.txt'):
            ticker_dict[symbol] = name
            ticker_count += 1
            if ticker_count % 1000 == 0:
                print(f"Loaded {ticker_count} tickers so far...")
        
        print(f"Loaded {len(ticker_dict)} tickers")

        if not start_processing and last_processed_symbol:
            tickers = list(ticker_dict.keys())
            try:
                start_index = tickers.index(last_processed_symbol) + 1
                print(f"Resuming from ticker #{start_index}: {last_processed_symbol}")
            except ValueError:
                print(f"Warning: Last processed symbol '{last_processed_symbol}' not found in ticker list. Starting from beginning.")
                start_index = 0
        else:
            start_index = 0
            print("Starting from the beginning of the ticker list")

        print("Loading previously identified top companies...")
        load_top_roic_companies('top_roic_companies.json')
        if top_roic_companies:
            print(f"Loaded {len(top_roic_companies)} previously identified companies")
        else:
            print("No previously identified companies found")

        # Start the hourly update thread
        total_tickers = len(ticker_dict)
        update_thread = threading.Thread(target=send_hourly_updates, args=(total_tickers,), daemon=True)
        update_thread.start()

        ticker_items = list(ticker_dict.items())[start_index:]
        total_batches = (len(ticker_items) + 9) // 10  # Calculate total number of batches (with batch size 10)
        
        print(f"Starting processing of {len(ticker_items)} tickers in {total_batches} batches")
        
        for i, batch in enumerate(batch_generator(ticker_items, n=10)):
            print(f"\nProcessing batch {i+1}/{total_batches} ({len(batch)} tickers)")
            process_batch(batch)
            if batch:  # Ensure batch is not empty before saving last processed symbol
                save_last_processed_symbol(batch[-1][0], 'last_processed.txt')  # Save last ticker of the batch
            
            # Sleep between batches to prevent API throttling
            if i < total_batches - 1:  # Don't sleep after the last batch
                time.sleep(5)

        print("\nScript finished processing all tickers.")
        if top_roic_companies:
            # Final sorting - first by ROIC (descending)
            top_roic_companies.sort(key=lambda x: x['ROIC'], reverse=True)
            # Get top 100 companies by ROIC
            top_companies = top_roic_companies[:100] if len(top_roic_companies) >= 100 else top_roic_companies
            # Then sort by Faustmann ratio (ascending)
            top_companies.sort(key=lambda x: x['Faustmann_Ratio'])
            # Get top 10 with lowest Faustmann ratio
            top_10_faustmann = top_companies[:10]
            
            message = "\n".join([
                f"Ticker: {item['Ticker']}, Company: {item['Company']}, Faustmann Ratio: {item['Faustmann_Ratio']}, ROIC: {item['ROIC']}, Debt Ratio: {item['Debt_Ratio']}"
                for item in top_10_faustmann
            ])
            print("\nTop 10 Companies with High ROIC and Low Faustmann Ratio (Final Result):")
            print(message)
            # send_telegram_message(GROUP_CHAT_ID, message) # Optionally send final results to Telegram
    except Exception as e:
        print(f"Critical error in main function: {e}")
        # Optionally, save our progress and top companies if we crash
        if top_roic_companies:
            save_top_roic_companies('top_roic_companies_emergency.json')
            print("Saved current top companies to emergency file")

# Call the main function to run the script
if __name__ == "__main__":
    main()