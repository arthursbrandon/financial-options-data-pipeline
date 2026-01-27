from etl.extract import schwab_api
from etl.transform import data_transformer
from dotenv import load_dotenv
import pandas as pd
import os

import json
import datetime
from datetime import datetime,date,time,timedelta

load_dotenv(dotenv_path='../Algorithmic_trading_system/util/.env')
api_key = os.getenv("APP_KEY")
api_secret = os.getenv("APP_SECRET")

#ETL Classes
api_extract = schwab_api.schwab(api_key,api_secret)
transformer = data_transformer.Data_Transformer()

ticker_list = ['SPY','QQQ','IWM','GLD','SLV','TQQQ']

try:

    #a = api_extract.getPriceHist('SPY')
    #df = pd.DataFrame.from_dict(a, orient='columns')
    #df = df['candles'].apply(pd.Series)
    
    
    a = api_extract.getOptionschain('SPY')
    df = pd.DataFrame.from_dict(a, orient='columns')
    exp_dates = df.loc[:,['callExpDateMap','putExpDateMap']].index
    #calls = df.iloc[0][['callExpDateMap','putExpDateMap']]
    types = ['callExpDateMap','putExpDateMap']
    
    for dates in exp_dates:
        x = df.loc[dates][['callExpDateMap','putExpDateMap']]
        for type in types:
            strikes = x[type].keys()
            for strike in strikes:
                print(x[type][strike][0])
        
        
    

    """
    #print(df.loc[df.index.isin(calls_dates),])
    calls = df['callExpDateMap']['2026-01-27:0'].keys
    print(calls.keys)
    print(calls)
    
    Index(['2026-01-27:0', '2026-01-28:1', '2026-01-29:2', '2026-01-30:3',
       '2026-02-02:6', '2026-02-03:7', '2026-02-04:8', '2026-02-05:9',
       '2026-02-06:10', '2026-02-09:13', '2026-02-13:17', '2026-02-20:24',
       '2026-02-27:31', '2026-03-06:38', '2026-03-20:52'],
      dtype='str')
    """

    
    """
    Index(['687.0', '688.0', '689.0', '690.0', '691.0', '692.0', '693.0', '694.0',
       '695.0', '696.0', '697.0', '698.0', '699.0', '665.0', '670.0', '675.0',
       '680.0', '685.0', '700.0', '705.0', '710.0', '715.0', '720.0', '725.0']
    """

except Exception as e:
    print(f'Error: {e}')







