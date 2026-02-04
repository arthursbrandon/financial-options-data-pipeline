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
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo

#API Details
load_dotenv(dotenv_path='../Algorithmic_trading_system/util/.env')
api_key = os.getenv("APP_KEY")
api_secret = os.getenv("APP_SECRET")

timestamp_ms = int(datetime.now(ZoneInfo("America/New_York")).replace(hour=9, minute=30, second=0, microsecond=0).timestamp())

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

def loadHistoricalDB():
   """
   Docstring for loadHistoricalDB
   Load historical data into sqlite database
   """
   if os.path.exists(db_path) == False:
       loader.load_to_db('stocks',historicalData,'historical_data')
   else:
       loader.update_db('stocks',historicalData,'historical_data')


def loadQuotesDB():
   """
   Docstring for loadHistoricalDB
   Load historical data into sqlite database
   """
   if os.path.exists(db_path) == False:
       loader.load_to_db('stocks',quoteData,'quote_data')
   else:
       loader.update_db('stocks',quoteData,'quote_data')

def loadOpenquotesDB():
   """
   Docstring for loadHistoricalDB
   Load historical data into sqlite database
   """
   if os.path.exists(db_path) == True:
       loader.load_to_db('stocks',quoteData,'opening_quote_data')


#Calculate net change since open for each ticker
def get_netChange():
   #Net change calculation
   timestamp_ms = int(datetime.now(ZoneInfo("America/New_York")).replace(hour=9, minute=30, second=0, microsecond=0).timestamp() * 1000)  
   initial_askPrice = historicalData_db.loc[(historicalData_db['symbol'].isin(ticker_list))& (historicalData_db['datetime'] == timestamp_ms)]['close'].values
   current_askPrice = quoteData.loc[quoteData.index.isin(ticker_list),'askPrice'].values
   net_change = current_askPrice - initial_askPrice/initial_askPrice * 100


   return net_change.round(2)

def get_low_net_avg(symbol_list):
   data = []
   historical_df = historicalData_db.set_index('symbol', drop=False)
   #Timestamp to mark the first market open in the data
   for ticker in symbol_list:
      df = historical_df[historical_df.index == ticker]
      df['date'] = pd.to_datetime(df['datetime'], unit = 'ms').dt.date
      df['time'] = pd.to_datetime(df['datetime'], unit = 'ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern').dt.time

      init_ask = df.loc[df['date'].isin(df['date']) & (df['time'] == pd.to_datetime('09:30:00').time()),'close'].values
      low_close = df.loc[
          df['date'].isin(df['date']) & (df['time'] > pd.to_datetime('09:30:00').time())
          & (df['time'] < pd.to_datetime('16:00:00').time())
          ].groupby('date', as_index=False).agg(lowest_close=('low', 'min'))['lowest_close'].values
      
      net_change = float(np.round(np.average(((low_close - init_ask)/init_ask)*100), decimals=2))

      data.append(net_change)
   return data

def get_high_net_avg(symbol_list):
  data = []
  historical_df = historicalData_db.set_index('symbol', drop=False)
   #Timestamp to mark the first market open in the data
  for ticker in symbol_list:
      df = historical_df[historical_df.index == ticker]
      df['date'] = pd.to_datetime(df['datetime'], unit = 'ms').dt.date
      df['time'] = pd.to_datetime(df['datetime'], unit = 'ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern').dt.time




      init_ask = df.loc[df['date'].isin(df['date']) & (df['time'] == pd.to_datetime('09:30:00').time()),'close'].values
      high_close = df.loc[
          df['date'].isin(df['date']) & (df['time'] > pd.to_datetime('09:30:00').time())
          & (df['time'] < pd.to_datetime('16:00:00').time())
          ].groupby('date', as_index=False).agg(highest_close=('high', 'max'))['highest_close'].values




      net_change = float(np.round(np.average(((high_close - init_ask)/init_ask)*100), decimals=2))
      data.append(net_change)
  return data


#Display Data
def displayData():
   net_change = get_netChange()
   high_net_avg = get_high_net_avg(ticker_list)
   low_net_avg = get_high_net_avg(ticker_list)
   for i in ticker_list:
       index_num = ticker_list.index(i)
       quotes = quoteData.loc[quoteData.index == i]
       print(f'\nTicker: {i}')
       print(f'Ask price: ${quotes['askPrice'].values[0]}')
       print(f'Net Change: {net_change[index_num]}%')
       print(f'Low Net change: {low_net_avg[index_num]}% | High Net change: {high_net_avg[index_num]}%')


def main():
   """
   Docstring for main
   1.) At 9:30 get opening volume and price, download historical data for each ticker in list
   2.) Define the month trend,low_net_avg,high_net_avg
   """
  
   while True:
      
       if market_open <= current_time and current_time >= last_db_update and os.path.getmtime(db_path) <  timestamp_ms:
       #if market_open <= current_time and current_time >= last_db_update:
           #Threading for loading data
           loadOpenquotes_thread = threading.Thread(target=loadOpenquotesDB)
           loadQuotes_thread =  threading.Thread(target=loadQuotesDB)
           loadHistorical_thread = threading.Thread(target=loadHistoricalDB)
          
           print("Market just opened!")

           loadOpenquotes_thread.start()
           loadQuotes_thread.start()
           loadHistorical_thread.start()

           loadOpenquotes_thread.join()
           loadQuotes_thread.join()
           loadHistorical_thread.join()

       elif market_close >= current_time:
           print("Market is closed for the day.")
           time.sleep(60)
       else:
           displayData()
       time.sleep(60)  # Check every minute

if __name__ == "__main__":
   main()
