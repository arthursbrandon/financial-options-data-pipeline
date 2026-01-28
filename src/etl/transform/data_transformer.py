from etl import *

class Data_Transformer:
    
    def __init__(self):
        pass
    
    def clean_quotes(self,quoteData):
        try:
            df = pd.DataFrame.from_dict(quoteData, orient='index')
            df = df['quote'].apply(pd.Series)
            return df
        except Exception as e:
            print(f'Error: {e}')
    
    def clean_historical(self,historicalData):
        try:
            df = pd.DataFrame.from_dict(historicalData, orient='columns')
            df = df['candles'].apply(pd.Series)
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