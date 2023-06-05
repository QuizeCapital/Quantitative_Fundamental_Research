print('Calculating Quintiles...')
from aggregate_functions import AggregateFunctions 
import pandas as pd

'''
Inputs: Dataframe with date, symbol, and calculation_column columns.
Modules called: aggregate_functions.py
Dates should be on an annual basis.
Outputs: List of Dataframe for each quintile with symbol and quintiles columns.
'''

class QuintileFunctions():

    def __init__(self, data_df, date_column, symbol_column, calculation_column, agg_function):
        '''
        Initialize the QuintileFunctions class.

        Args:
            data_df (DataFrame): Input dataframe with required columns.
            date_column (str): Name of the column containing dates.
            symbol_column (str): Name of the column containing symbols.
            calculation_column (str): Name of the column to calculate quintiles on.
            agg_function (str or callable): Aggregation function to apply.

        '''
        self.data_df = data_df
        self.date_column = date_column
        self.symbol_column = symbol_column
        self.calculation_column = calculation_column
        self.agg_function = agg_function

    def aggregate_data(self):
        '''
        Aggregate the data based on the specified columns and aggregation function.

        Returns:
            DataFrame: Aggregated dataframe with required columns.

        '''
        return AggregateFunctions(
            self.data_df, self.date_column, self.symbol_column, self.calculation_column, self.agg_function
        ).transform_data()

    def get_quintile_groups(self):
        '''
        Get the quintile groups based on the specified calculation column.

        Returns:
            list: List of dataframes, each dataframe representing a quintile group with symbol and quintiles columns.

        '''
        df = self.aggregate_data()
        df['quintiles'] = pd.qcut(df[self.calculation_column], q=5, labels=False)
        return [group for _, group in df.groupby('quintiles')]

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

    print(quintile_groups)


