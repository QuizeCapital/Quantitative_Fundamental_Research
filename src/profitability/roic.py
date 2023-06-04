print('Running roic.py...')
import re
import sys
from pathlib import Path

# tools_path = os.path.abspath(os.path.join(
#         __file__, '..', '..', 'tools'))
# if tools_path not in sys.path:
#     sys.path.append(tools_path)
tools_path = Path(__file__).resolve().parent.parent / 'tools'
sys.path.append(str(tools_path))

from cagr import CAGR
from quintile_functions import QuintileFunctions
import pandas as pd



class ROIC:
    """
    Class for calculating ROIC (Return on Invested Capital) related metrics.

    Parameters:
    - data_df_returns (pd.DataFrame): DataFrame containing return data
    - data_df_ratios (pd.DataFrame): DataFrame containing ratio data
    - date_column (str): Name of the column in the data DataFrame representing dates
    - return_column (str): Name of the column in the return DataFrame representing returns
    - symbol_column (str): Name of the column in both DataFrames representing symbols
    - agg_function (str or callable): Aggregation function to use when calculating quintiles
    - calc_column (str): Name of the column in the ratio DataFrame to calculate quintiles on
    """

    def __init__(self, data_df_returns, data_df_ratios, date_column, return_column, symbol_column, agg_function, calc_column):
        self.data_df_returns = data_df_returns
        self.data_df_ratios = data_df_ratios
        self.date_column = date_column
        self.return_column = return_column
        self.symbol_column = symbol_column
        self.agg_function = agg_function
        self.calc_column = calc_column

    def get_symbols_in_each_quintile_ROIC(self):
        """
        Calculate the symbols in each quintile based on ROIC.

        Returns:
        - quintile_symbols (dict): Dictionary where keys represent quintiles and values are lists of symbols in each quintile
        """
        quintiles = QuintileFunctions(self.data_df_ratios, self.date_column, self.symbol_column, self.calc_column, self.agg_function).get_quintile_groups()

        quintile_symbols = {}
        for quintile, group in quintiles:
            quintile_symbols[quintile] = group[self.symbol_column].tolist()

        return quintile_symbols

    def get_cagr(self):
        """
        Calculate the Compound Annual Growth Rate (CAGR) for each symbol.

        Returns:
        - cagr (pd.DataFrame): DataFrame with CAGR values for each symbol
        """
        return CAGR(self.data_df_returns, self.date_column, self.return_column, self.symbol_column).final_df()

    def get_cagr_for_each_quintile_ROIC(self):
        """
        Calculate the average CAGR for each quintile based on ROIC.

        Returns:
        - cagr_quintile (dict): Dictionary where keys represent quintiles and values are the average CAGR for each quintile
        """
        quintile_symbols = self.get_symbols_in_each_quintile_ROIC()
        cagr = self.get_cagr()

        cagr_quintile = {}
        for quintile, symbols in quintile_symbols.items():
            cagr_quintile[quintile] = cagr[cagr[self.symbol_column].isin(symbols)][['cagr']].mean().values[0]

        return cagr_quintile

    def get_cagr_for_universe(self):
        """
        Calculate the average CAGR for the entire universe.

        Returns:
        - cagr_universe (float): Average CAGR for the entire universe
        """
        cagr = self.get_cagr()
        return cagr[['cagr']].mean().values[0]





if __name__ == "__main__":
    import os
    from adhoc_tools import AdhocTools


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
    # quintile_symbols = roic_obj.get_symbols_in_each_quintile_ROIC()
    # print(quintile_symbols)

    # cagr = roic_obj.get_cagr()
    # print(cagr.head())

    cagr_quintile = roic_obj.get_cagr_for_each_quintile_ROIC()
    print(cagr_quintile)

    cagr_universe = roic_obj.get_cagr_for_universe()
    print(cagr_universe)

    