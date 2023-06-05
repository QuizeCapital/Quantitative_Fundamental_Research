import pandas as pd
from datetime import date
import os
print('Aggregating columns in a dataframe based on a symbol column')


'''
This class calculates the aggregate of a column in a dataframe.
Inputs: Dataframe with date, symbol, and calculation_column columns.
Dates should be on an annual basis.
Outputs: Dataframe with symbol,agg_function columns.
'''


class AggregateFunctions():

    def __init__(self, data_df, date_column, symbol, calculation_column, agg_function):
        self.data_df = data_df
        self.date_column = date_column
        self.symbol = symbol
        self.calculation_column = calculation_column
        self.agg_function = agg_function

    def transform_data(self):
        df = self.data_df
        return (
            df.groupby(self.symbol)
            .agg({self.calculation_column: self.agg_function})
            .reset_index()
        )

    def return_agg_df(self):
        '''
        Returns a dataframe with the aggregated data with the following columns:
        - symbol
        - calculation_column which is the column that was aggregated
        '''
        return self.transform_data()


if __name__ == "__main__":
    from adhoc_tools import AdhocTools

    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'financial_ratios.xlsx'))

    df = AdhocTools(path).read_data()
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'symbol', 'roic']]
    print(df.head())

    date_column = 'date'
    symbol = 'symbol'
    calculation_column = 'roic'
    agg_function = 'mean'

    df = AggregateFunctions(
        df, date_column, symbol, calculation_column, agg_function
    ).return_agg_df()
    print(df.head())
