from etl.extract import schwab_api
from etl.transform import data_transformer
from etl.load import data_loader
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os

#API Details
load_dotenv(dotenv_path='../Algorithmic_trading_system/util/.env')
api_key = os.getenv("APP_KEY")
api_secret = os.getenv("APP_SECRET")

#ETL Classes
api_extract = schwab_api.schwab(api_key,api_secret)
transformer = data_transformer.Data_Transformer()
loader = data_loader.Data_Loader()

#Database Path
db_path = '../Algorithmic_trading_system/src/db/stocks.db'

# Market time variables
current_time = datetime.now().timestamp()
market_open = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0).timestamp()
market_close = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0).timestamp()

#DB modification timestamp
last_db_update = os.path.getmtime(db_path) if os.path.exists(db_path) else 0

#List of tickers and data extraction
ticker_list = ['SPY','QQQ','IWM','GLD','SLV']
quoteData = transformer.clean_quotes(api_extract.getQuotes(ticker_list))
historicalData = transformer.clean_historical(api_extract.getPriceHist(ticker_list))

#Data from DB
openingQuoteData = loader.read_from_db('stocks','opening_quote_data')
historicalData_db = loader.read_from_db('stocks','historical_data')

