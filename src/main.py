from etl.extract import schwab_api
from etl.transform import data_transformer
from dotenv import load_dotenv
import os

#API Details
load_dotenv(dotenv_path='../Algorithmic_trading_system/util/.env')
api_key = os.getenv("APP_KEY")
api_secret = os.getenv("APP_SECRET")

#ETL Classes
api_extract = schwab_api.schwab(api_key,api_secret)
transformer = data_transformer.Data_Transformer()

#List of tickers
ticker_list = ['SPY','QQQ','IWM','GLD','SLV']

#quoteData = transformer.clean_quotes(api_extract.getQuotes(ticker_list))
def main():
    """
    Docstring for main

    1.) At 9:30 get opening volume and price, download historical data for each ticker in list
    3.) Define the month trend,low_net_avg,high_net_avg
    4.)
    """

if __name__ == "__main__":








