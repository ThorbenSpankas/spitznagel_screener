import yfinance as yf
import pandas as pd
import logging
import ast
import gc
import requests

# Configure logging
logger = logging.getLogger('yfinance')
logger.setLevel(logging.DEBUG)  # Adjust level as necessary
# Initialize the Telegram Bot with your token
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

def send_telegram_message(chat_id, text):
    """Sends a message to the specified Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        'chat_id': chat_id,
        'text': text
    }
    response = requests.post(url, data=data)
    return response.json()

# Function to process each ticker and calculate the necessary financial ratios
def process_ticker(ticker_symbol, company_name):
    ticker = yf.Ticker(ticker_symbol)
    try:
        market_cap = ticker.info.get("marketCap", 0)
        balance_sheet = ticker.balance_sheet

        # Check if necessary data is available
        if market_cap > 0 and 'Invested Capital' in balance_sheet.index and 'Cash And Cash Equivalents' in balance_sheet.index and 'Total Debt' in balance_sheet.index:
            invested_capital_current = balance_sheet.loc['Invested Capital'].iloc[0]
            total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0]
            total_debt = balance_sheet.loc['Total Debt'].iloc[0]
            
            # Check if Preferred Stock is available, otherwise set it to 0
            if 'Preferred Stock' in balance_sheet.index:
                preferred_equity = balance_sheet.loc['Preferred Stock'].iloc[0]
            else:
                preferred_equity = 0

            below_debt_limit = invested_capital_current + total_cash > total_debt + preferred_equity
            # Calculate the Faustmann ratio
            faustmann_ratio = round(market_cap / (invested_capital_current + total_cash - total_debt - preferred_equity), 3)

            # Check if Faustmann ratio is below 1
            if below_debt_limit:
                ebit = ticker.financials.loc["EBIT"].iloc[0]
                roic = round(ebit / invested_capital_current, 3)

                if roic > 0.3:
                    # Add to top ROIC companies list if not full, else replace the lowest ROIC if the new one is higher
                    if len(top_roic_companies) < 30:
                        top_roic_companies.append({"Ticker": ticker_symbol, "Company": company_name, "Faustmann_Ratio": faustmann_ratio, "ROIC": roic})
                    else:
                        min_roic = min(top_roic_companies, key=lambda x: x['ROIC'])
                        if roic > min_roic['ROIC']:
                            top_roic_companies.remove(min_roic)
                            top_roic_companies.append({"Ticker": ticker_symbol, "Company": company_name, "Faustmann_Ratio": faustmann_ratio, "ROIC": roic})

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

# Main logic to parse and process tickers
def main():
    last_processed_symbol = get_last_processed_symbol('last_processed.txt')
    start_processing = False if last_processed_symbol else True

    for key, value in parse_large_dict('ticker_list_yf.txt'):
        if start_processing:
            print(f"Symbol: {key}, Name: {value}")
            process_ticker(key, value)
            save_last_processed_symbol(key, 'last_processed.txt')
        elif key == last_processed_symbol:
            start_processing = True

# Call the main function to run the script
if __name__ == "__main__":
    main()