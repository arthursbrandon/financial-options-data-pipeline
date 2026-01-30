from etl.extract import schwab_api
from etl.transform import data_transformer
from etl.load import data_loader
from dotenv import load_dotenv
import os
import os.path
from datetime import datetime,timedelta,time
import pandas as pd
import threading
import time


#API Details
load_dotenv(dotenv_path='../Algorithmic_trading_system/util/.env')
api_key = os.getenv("APP_KEY")
api_secret = os.getenv("APP_SECRET")

#ETL Classes
api_extract = schwab_api.schwab(api_key,api_secret)
transformer = data_transformer.Data_Transformer()
loader = data_loader.Data_Loader()

#List of tickers and data extraction
ticker_list = ['SPY','QQQ','IWM','GLD','SLV']
quoteData = transformer.clean_quotes(api_extract.getQuotes(ticker_list))
historicalData = transformer.clean_historical(api_extract.getPriceHist(ticker_list))

# Market time variables
current_time = datetime.now().timestamp()
market_open = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0).timestamp()
market_close = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0).timestamp()

def loadHistoricalDB():
    """
    Docstring for loadHistoricalDB
    Load historical data into sqlite database
    """
    if os.path.exists('../Algorithmic_trading_system/src/db/stocks.db') == False:
        print('\nLoading Historical Table')
        loader.load_to_db('stocks',historicalData,'historical_data')
        print('\nHistorical Table Updated')
    else:
        print('\nHistorical Table already exists, updating with new data')
        loader.update_db('stocks',historicalData,'historical_data')
        print('\nHistorical Table Updated')

def loadQuotesDB():
    """
    Docstring for loadHistoricalDB
    Load historical data into sqlite database
    """
    if os.path.exists('../Algorithmic_trading_system/src/db/stocks.db') == False:
        print('\nLoading Quotes Table')
        loader.load_to_db('stocks',quoteData,'quote_data')
        print('\nHistorical Database Updated')
    else:
        print('\nQuotes Table already exists, updating with new data')
        loader.update_db('stocks',quoteData,'quote_data')
        print('\nQuotes Table Updated')

def metrics():
    d = {}
    pass
def main():
    """
    Docstring for main
    1.) At 9:30 get opening volume and price, download historical data for each ticker in list
    2.) Define the month trend,low_net_avg,high_net_avg
    """
    #Threading for loading data
    loadQuotes_thread =  threading.Thread(target=loadQuotesDB)
    loadHistorical_thread = threading.Thread(target=loadHistoricalDB)
    
    while True:
        if market_open == current_time:
            
            print("Market just opened!")

            loadQuotes_thread.start()
            loadHistorical_thread.start()

            loadQuotes_thread.join()
            loadHistorical_thread.join()

        elif market_close >= current_time:
            print(market_close)
            print("Market just closed!")
        else:
            print("Market in progress.")
        time.sleep(60)  # Check every minute



if __name__ == "__main__":
    main()






