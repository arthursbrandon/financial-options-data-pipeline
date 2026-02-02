from etl import *

class Data_Transformer:
    
    def __init__(self):
        pass
    
    def clean_quotes(self,quoteData):
        try:
            df = pd.DataFrame.from_dict(quoteData, orient='index')
            df = df['quote'].apply(pd.Series)
            df['tradeTime'] = pd.to_datetime(df['tradeTime'], unit='ms')
            df['tradeTime'] = df['tradeTime'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            df['tradeTime'] = df['tradeTime'].dt.strftime('%Y-%m-%d %I:%M %p')
            df.index.name = 'symbol'
            return df
        except Exception as e:
            print(f'Error: {e}')
    
    def clean_historical(self,historicalData):
        try:
            
            data = [pd.DataFrame(d) for d in historicalData]
            # Combine into one dataframe
            df = pd.concat(data, ignore_index=True)
            # Expand the candles dict into columns
            candles_df = pd.json_normalize(df['candles'])
            # Merge back to original dataframe
            df = pd.concat([df.drop(columns='candles'), candles_df], axis=1)
            # Convert date time from epoch to human readable format
            df[f'datetime_readable'] = pd.to_datetime(df['datetime'], unit='ms')
            df['datetime_readable'] = df['datetime_readable'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            df['datetime_readable'] = df['datetime_readable'].dt.strftime('%Y-%m-%d %I:%M %p')
            return df
        
        except Exception as e:
            print(f'Error: {e}')

    def clean_Options(self, optionData):
        data = []

        df = pd.DataFrame.from_dict(optionData, orient='columns')
        exp_dates = df.loc[:,['callExpDateMap','putExpDateMap']].index
        types = ['callExpDateMap','putExpDateMap']

        for dates in exp_dates:
            x = df.loc[dates][['callExpDateMap','putExpDateMap']]
            for type in types:
                strikes = x[type].keys()
                for strike in strikes:
                    optionData.append(x[type][strike][0])

        df = pd.DataFrame(data).drop(columns='optionDeliverablesList')
        df['expirationDate'] = pd.to_datetime(df['expirationDate']).dt.date
        return df