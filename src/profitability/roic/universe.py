print('Calculate Data for Investment Universe')
import sys
from pathlib import Path
tools_path = Path(__file__).resolve().parent.parent / 'tools'
sys.path.append(str(tools_path))
import pandas as pd

from cagr import CAGR
class Universe():

    def __init__(self, data_df, date_column, symbol_column, calculation_column, agg_function):
        '''
        Initialize the class with the required parameters.

        Args:
            data_df (DataFrame): DataFrame containing the required data.
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

    def calculate_CAGR(self):

        cagr =  CAGR(self.data_df, self.date_column, self.calculation_column, self.symbol_column).final_df()
        
        return cagr[['cagr']].mean()
    
if __name__ == "__main__":
    import os
    from adhoc_tools import AdhocTools


    date_column = 'date'
    symbol = 'symbol'
    agg_function = 'mean'

    path_returns = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'annual_historical_returns(sym_ticker,return).xlsx'))
    
    df = AdhocTools(path_returns).read_data()
    df['year'] = pd.to_datetime(df['year'])
    df = df.rename(columns={'year': 'date'})
    df = df.rename(columns={'cum_pct_ch_year': 'annual_return'})
    data_df_returns = df[['date', 'symbol', 'annual_return']]

    return_column = 'annual_return'

    universe_obj = Universe(data_df_returns, date_column, symbol, return_column, agg_function)

    cagr = universe_obj.calculate_CAGR()
    print(cagr)