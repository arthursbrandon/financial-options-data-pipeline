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