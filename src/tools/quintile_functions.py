print('Calculating Quintiles...')
from aggregate_functions import AggregateFunctions 
import pandas as pd

'''
Inputs: Dataframe with date, symbol, and calculation_column columns.
Modules called: aggregate_functions.py
Dates should be on an annual basis.
Outputs: list of  Dataframe for each quintile with symbol and quintiles columns.
'''

class QuintileFunctions():

    def __init__(self, data_df, date_column, symbol, calculation_column, agg_function):
        self.data_df = data_df
        self.date_column = date_column
        self.symbol = symbol 
        self.calculation_column = calculation_column #also quintitle column
        self.agg_function = agg_function

    def aggregate_data(self):
        return AggregateFunctions(
        self.data_df, self.date_column, self.symbol, self.calculation_column, self.agg_function
    ).return_agg_df()

    def get_quintile_groups(self):
        '''
        This function returns a groupby object with the quintiles as the index.
        '''

        df = self.aggregate_data()
        df['quintiles'] = pd.qcut(df[self.calculation_column], q=5, labels=False)
        return df.groupby('quintiles')
    

if __name__ == "__main__":
    import os
    from adhoc_tools import AdhocTools


    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'financial_ratios.xlsx'))

    df = AdhocTools(path).read_data()
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'symbol', 'roic']]

    date_column = 'date'
    symbol = 'symbol'
    calculation_column = 'roic'
    agg_function = 'mean'

    quintile_groups = QuintileFunctions(
        df, date_column, symbol, calculation_column, agg_function
    ).get_quintile_groups()

    for quintile, group in quintile_groups:
        print(quintile)
        print(group.head())
        print()


