from tools.cagr import CAGR
from tools.quintile_functions import QuintileFunctions
import pandas as pd

class roic():

    def __init__(self, data_df_returns, data_df_ratios, date_column, return_column, symbol_column, agg_function, calc_column):
        self.data_df_returns = data_df_returns
        self.data_df_ratios = data_df_ratios
        self.date_column = date_column
        self.return_column = return_column
        self.symbol_column = symbol_column
        self.agg_function = agg_function
        self.calc_column = calc_column

    def get_symbols_in_each_quintile_ROIC(self):

        quintiles = QuintileFunctions(
        self.data_df_ratios, self.date_column, self.symbol_column, self.calc_column, self.agg_function
    ).get_quintile_groups()
        
        quintile_symbols = {}
        for quintile, group in quintiles:
            quintile_symbols[quintile] = group[self.symbol_column].tolist()

        return quintile_symbols


if __name__ == "__main__":
    import os
    from tools.adhoc_tools import AdhocTools


    path_ratios = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'financial_ratios.xlsx'))

    data_df_ratios = AdhocTools(path_ratios).read_data()
    data_df_ratios['date'] = pd.to_datetime(data_df_ratios['date'])
    data_df_ratios = data_df_ratios[['date', 'symbol', 'roic']]

    date_column = 'date'
    symbol = 'symbol'
    calculation_column = 'roic'
    agg_function = 'mean'

    path_returns = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'annual_historical_returns(sym_ticker,return).xlsx'))
    
    df = AdhocTools(path_returns).read_data()
    df['year'] = pd.to_datetime(df['year'])
    df = df.rename(columns={'year': 'date'})
    df = df.rename(columns={'cum_pct_ch_year': 'annual_return'})
    data_df_returns = df[['date', 'symbol', 'annual_return']]

    return_column = 'annual_return'

    roic_obj = roic(data_df_returns, data_df_ratios, date_column, return_column, symbol, agg_function, calculation_column)
    quintile_symbols = roic_obj.get_symbols_in_each_quintile_ROIC()
    quintile_symbols

    