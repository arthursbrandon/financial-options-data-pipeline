from etl.extract import schwab_api
from etl.transform import data_transformer
from etl.load import data_loader
from dotenv import load_dotenv
import os
from datetime import datetime,timedelta,time
import pandas as pd


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


def main():
    
    pass

if __name__ == "__main__":
    main()






