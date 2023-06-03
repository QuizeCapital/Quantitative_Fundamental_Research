print('Aggregating functions...')
from datetime import date
import os

import pandas as pd
from adhoc_tools import AdhocTools

class AggregateFunctions():

    def __init__(self, data_path, date_column, needed_columns, group_by_column, calculation_column, agg_function):
        self.data_path = data_path
        self.date_column = date_column
        self.needed_columns = needed_columns
        self.group_by_column = group_by_column
        self.calculation_column = calculation_column
        self.agg_function = agg_function

    def read_data(self):
        return AdhocTools(self.data_path).read_data()
    
    def get_dataframe_needed(self):
        df = self.read_data()
        df = df[self.needed_columns]
        df[self.date_column] = pd.to_datetime(df[self.date_column])
        return df
    
    def transform_data(self):
        df = self.get_dataframe_needed()
        df = df.groupby(self.group_by_column).agg(
            {self.calculation_column: self.agg_function}
        ).reset_index()
        return df
    
    def return_agg_df(self):
        '''
        Returns a dataframe with the aggregated data with the following columns:
        - symbol
        - calculation_column which is the column that was aggregated
        '''
        return self.transform_data()

    
if __name__ == "__main__":
    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'financial_ratios.xlsx'))
    
    date_column = 'date'
    needed_columns = ['date', 'symbol', 'roic']
    group_by_column = 'symbol'
    calculation_column = 'roic'
    agg_function = 'mean'
    
    df = AggregateFunctions(
        path, date_column, needed_columns, group_by_column, calculation_column, agg_function
    ).return_agg_df()
    print(df.shape)
