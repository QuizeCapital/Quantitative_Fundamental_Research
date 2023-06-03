print('Importing Adhoc Tools libraries...')
import pandas as pd
import os

class AdhocTools():

    def __init__(self, df_path):
        self.df_path = df_path

    def read_data(self):

        if self.df_path.endswith('.csv'):
            print('Reading csv file...')
            return pd.read_csv(self.df_path)
        elif self.df_path.endswith('.xlsx'):
            print('Reading excel file...')
            return pd.read_excel(self.df_path)
        elif self.df_path.endswith('.json'):
            print('Reading json file...')
            return pd.read_json(self.df_path)
        elif self.df_path.endswith('.parquet'):
            print('Reading parquet file...')
            return pd.read_parquet(self.df_path)

if __name__ == "__main__":
    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'S&P_historical_price.parquet'))
    
    
    df = AdhocTools(path).read_data()
    print(df.head())
