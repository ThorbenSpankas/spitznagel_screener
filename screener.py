import yfinance as yf
import pandas as pd
import logging
import ast
import gc

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
with open("ticker_list_yf.txt", "r") as file:
    content = file.read()
    tickers_dict = ast.literal_eval(content)  # Safely evaluate the string as a dictionary

# Extract only the tickers
tickers = list(tickers_dict.keys())
# Initialize an empty list to store dictionaries
spitznagel_worthy = []

# Iterate over tickers and calculate the Faustmann ratio
print("lets go")

batch_size = 100  # Define how many tickers to process at a time

# Iterate over tickers in batches
for i in range(0, len(tickers), batch_size):
    batch_tickers = tickers[i:i+batch_size]
    for ticker_symbol in batch_tickers:
        ticker = yf.Ticker(ticker_symbol)
        try:
            market_cap = ticker.info.get("marketCap", 0)
            balance_sheet = ticker.balance_sheet
    
            # Check if necessary data is available
            if market_cap > 0 and 'Invested Capital' in balance_sheet.index and 'Cash And Cash Equivalents' in balance_sheet.index and 'Total Debt' in balance_sheet.index:
                invested_capital = balance_sheet.loc['Invested Capital'].iloc[0]
                total_cash = balance_sheet.loc['Cash And Cash Equivalents'].iloc[0]
                total_debt = balance_sheet.loc['Total Debt'].iloc[0]
    
                below_debt_limit = invested_capital + total_cash > total_debt
                # Calculate the Faustmann ratio
                faustmann_ratio = market_cap / (invested_capital + total_cash - total_debt)
    
                # Check if Faustmann ratio is below 1
                if faustmann_ratio < 1 and below_debt_limit:
                    # Add to the list as a dictionary
                    # spitznagel_worthy.append({"Ticker": ticker_symbol, "Faustmann Ratio": faustmann_ratio})
                   #  print(f"Ticker: {ticker_symbol}, Faustmann Ratio: {faustmann_ratio}, checking roic")
                    ebit = ticker.financials.loc["EBIT"].iloc[0]
                    roic = ebit/invested_capital
                    #print(f"Ticker: {ticker_symbol}, Faustmann Ratio: {faustmann_ratio}, ROIC: {roic}")
                    if roic > 0.75:
                        spitznagel_worthy.append({"Ticker": ticker_symbol, "Faustmann_Ratio": faustmann_ratio, "ROIC": roic})
                        #print("FOUND ONE")
                        print(f"Ticker: {ticker_symbol}, Faustmann Ratio: {faustmann_ratio}, ROIC: {roic}")
                        #print("FOUND ONE")
            
    
        except requests.exceptions.HTTPError as e:
            if e.status_code == 404:
                a= 33
    
        except requests.exceptions.HTTPError as err:
            a=44
    
        except requests.exceptions.RequestException as err:
            c = 309
        
        except KeyError:
            # Handle specific missing data errors quietly or log them
            a = 2# logging.info(f"Data missing for {ticker_symbol}")
    
        except IndexError:
            # Handle cases where .iloc[] fails due to missing data
            a = 3 #logging.info(f"Index error for {ticker_symbol}, might be missing financial data")
        
        except Exception as e:
            a = 1
            #print(f"Error processing {ticker_symbol}: {e}")

    gc.collect()


# Create DataFrame from the list of dictionaries
spitznagel_worthy = pd.DataFrame(spitznagel_worthy)

# Save the DataFrame with the results to a CSV file
spitznagel_worthy.to_csv("spitznagel_worthy.csv", index=False)

invalid_tickers = not_found_handler.invalid_tickers
for ticker in invalid_tickers:
    tickers.pop(ticker, None)
