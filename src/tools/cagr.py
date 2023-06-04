import os

import pandas as pd
from adhoc_tools import AdhocTools


class CAGR():

    def __init__(self, data_path, date_column, frequency, needed_columns, group_by_column, calculation_column):
        self.data_path = data_path
        self.date_column = date_column
        self.needed_columns = needed_columns
        self.frequency = frequency
        self.group_by_column = group_by_column
        self.calculation_column = calculation_column

    def read_data(self):
        return AdhocTools(self.data_path).read_data()
    
    def get_dataframe_needed(self):
        df = self.read_data()
        df = df[self.needed_columns]
        df[self.date_column] = pd.to_datetime(df[self.date_column])
        return df
    

    
    # def CAGR_formula(self, start_value, end_value, years):
    #     return (end_value / start_value) ** (1 / years) - 1
    
    # def transform_data(self):
    #     df = self.get_dataframe_needed()
    #     # group by group_by_column and sort by frequency in date_column ascending

    



if __name__ == "__main__":
    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'historical_price.parquet'))
    
    date_column = 'date'
    frequency = 'daily'
    needed_columns = ['date', 'symbol', 'close']
        
    
    df = CAGR(path).get_dataframe_needed()
    print(df.head())
