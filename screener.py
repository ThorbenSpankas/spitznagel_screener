import yfinance as yf
import pandas as pd
import logging
import ast
import gc
import requests

# Initialize the Telegram Bot with your token
TELEGRAM_TOKEN='6717990254:AAGFOAqjtHJ7gRD0enLdQvCkIFvJTtFOzYM'
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

# data from https://github.com/mlapenna7/yh_symbol_universe/blob/main/yhallsym.txt
# Load tickers from the file
# with open("ticker_list_yf.txt", "r") as file:
#     content = file.read()
#     tickers_dict = ast.literal_eval(content)  # Safely evaluate the string as a dictionary

# # Extract only the tickers
# tickers = list(tickers_dict.keys())
# Initialize an empty list to store dictionaries
spitznagel_worthy = []

def send_telegram_message(chat_id, text):
    """Sends a message to the specified Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        'chat_id': chat_id,
        'text': text
    }
    response = requests.post(url, data=data)
    return response.json()

# Iterate over tickers and calculate the Faustmann ratio
print("lets go")


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
                    spitznagel_worthy.append({"Ticker": ticker_symbol, "Faustmann_Ratio": faustmann_ratio, "ROIC": roic})
                    print(f"Ticker: {ticker_symbol}, Company: {company_name}, Faustmann Ratio: {faustmann_ratio}, ROIC: {roic}")
                    message = f"Ticker: {ticker_symbol}, Company: {company_name}, Faustmann Ratio: {faustmann_ratio}, ROIC: {roic}"
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