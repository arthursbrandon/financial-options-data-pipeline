from etl import *

class Data_Loader:
    
    def __init__(self):
        pass
    
    #Load dataframe to sqlite database
    def load_to_db(self,database,data_frame,table_name):
        conn = sqlite3.connect(f'../Algorithmic_trading_system/src/db/{database}.db')
        data_frame.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
    
    #Read dataframe from sqlite database
    def read_from_db(self,database,table_name):
        conn = sqlite3.connect(f'../Algorithmic_trading_system/src/db/{database}.db')
        df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
        conn.close()
        return df
    
    #Update existing records in the database
    def update_db(self,database,data_frame,table_name,key_column):
        conn = sqlite3.connect(f'../Algorithmic_trading_system/src/db/{database}.db')
        cursor = conn.cursor()
        conn.update(data_frame, table_name, key=key_column)
        conn.commit()