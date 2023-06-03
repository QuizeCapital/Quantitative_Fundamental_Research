print('Importing Quintile Functions libraries...')
from aggregate_functions import AggregateFunctions 
import pandas as pd

class QuintileFunctions():

    def __init__(self, data_path, date_column, needed_columns, group_by_column, calculation_column, agg_function):
        self.data_path = data_path
        self.date_column = date_column
        self.needed_columns = needed_columns
        self.group_by_column = group_by_column #also symbol column
        self.calculation_column = calculation_column #also quintitle column
        self.agg_function = agg_function

    def aggregate_data(self):
        return AggregateFunctions(
            self.data_path,
            self.date_column,
            self.needed_columns,
            self.group_by_column,
            self.calculation_column,
            self.agg_function,
        ).return_agg_df()

    def get_quintile_groups(self):

        df = self.aggregate_data()
        df['quintiles'] = pd.qcut(df[self.calculation_column], q=5, labels=False)
        return df.groupby('quintiles')
    

if __name__ == "__main__":
    import os

    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'financial_ratios.xlsx'))
    
    date_column = 'date'
    needed_columns = ['date', 'symbol', 'roic']
    group_by_column = 'symbol'
    calculation_column = 'roic'
    agg_function = 'mean'

    quintile_groups = QuintileFunctions(
        path, date_column, needed_columns, group_by_column, calculation_column, agg_function
    ).get_quintile_groups()

    for name, group in quintile_groups:
        print("Quintile:", name)
        print(group)
        print()


