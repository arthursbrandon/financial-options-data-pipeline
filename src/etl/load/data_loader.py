from etl import *

class Data_Loader:
    
    def __init__(self):
        pass
    
    #Load dataframe to sqlite database
    def load_to_db(self,database,data_frame,table_name):
        try:
            print(f'\n- Loading data to {database} database, table: {table_name}')
            conn = sqlite3.connect(f'../Algorithmic_trading_system/src/db/{database}.db')
            data_frame.to_sql(table_name, conn, if_exists='replace', index=True, index_label=None)
            conn.commit()
            conn.close()
            print(f'Loading data to {database} database, table: {table_name} - Completed')
        except Exception as e:
            print(f'Error updating data in database: {e}, table: {table_name}')
    
    #Read dataframe from sqlite database
    def read_from_db(self,database,table_name):
        try:
            print(f'\n- Reading data from {database} database, table: {table_name}')
            conn = sqlite3.connect(f'../Algorithmic_trading_system/src/db/{database}.db')
            df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
            conn.close()
            print(f'Reading data from {database} database, table: {table_name} - Completed')
            return df
        except Exception as e:
            print(f'Error updating data in database: {e}, table: {table_name}')
            return None
    
    #Update existing records in the database
    def update_db(self,database,data_frame,table_name):
        try:
            print(f'\n- Updating data in {database} database, table: {table_name}')
            conn = sqlite3.connect(f'../Algorithmic_trading_system/src/db/{database}.db')
            data_frame.to_sql(table_name, conn, if_exists='append', index=True, index_label=None)
            conn.commit()
            conn.close()
            print(f'Updating data in {database} database, table: {table_name} - Completed')
        except Exception as e:
            print(f'Error updating data in database: {e}, table: {table_name}')
            return None
        

        