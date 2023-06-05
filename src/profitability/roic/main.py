import re
import sys
from pathlib import Path

import pandas as pd
from cagr import CAGR
from quintile_functions import QuintileFunctions

print('Running roic.py...')

# tools_path = os.path.abspath(os.path.join(
#         __file__, '..', '..', 'tools'))
# if tools_path not in sys.path:
#     sys.path.append(tools_path)
tools_path = Path(__file__).resolve().parent.parent / 'tools'
sys.path.append(str(tools_path))


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

    def __init__(self, data_df_returns, data_df_ratios, date_column, return_column, symbol_column, agg_function, calc_column, cagr_column):
        self.data_df_returns = data_df_returns
        self.data_df_ratios = data_df_ratios
        self.date_column = date_column
        self.return_column = return_column
        self.symbol_column = symbol_column
        self.agg_function = agg_function
        self.calc_column = calc_column
        self.cagr_column = cagr_column

    def get_cagr_as_df(self):
        """
        Calculate the average Compound Annual Growth Rate (CAGR) for each quintile based on Return on Invested Capital (ROIC)
        and return it as a DataFrame.

        Returns:
        - cagr_df (pd.DataFrame): DataFrame with CAGR values for each quintile, sorted in descending order.
        """
        quintile_func = QuintileFunctions(self.data_df_ratios, self.date_column,
                                          self.symbol_column, self.calc_column, self.agg_function).get_quintile_groups()

        cagr = CAGR(self.data_df_returns, self.date_column,
                    self.return_column, self.symbol_column).final_df()

        cagr_quintile = {
            quintile: cagr[cagr[self.symbol_column].isin(symbols)][[
                self.cagr_column]]
            .mean()
            .values[0]
            for quintile, symbols in quintile_func.items()
        }

        sorted_data = dict(sorted(cagr_quintile.items(),
                           key=lambda x: x[1], reverse=True))

        return pd.DataFrame(sorted_data.values(), columns=["Quintiles"]).T



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
    cagr_column = 'cagr'

    path_returns = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'annual_historical_returns(sym_ticker,return).xlsx'))

    df = AdhocTools(path_returns).read_data()
    df['year'] = pd.to_datetime(df['year'])
    df = df.rename(columns={'year': 'date'})
    df = df.rename(columns={'cum_pct_ch_year': 'annual_return'})
    data_df_returns = df[['date', 'symbol', 'annual_return']]

    return_column = 'annual_return'

    roic_obj = ROIC(data_df_returns, data_df_ratios, date_column,
                    return_column, symbol, agg_function, calculation_column, cagr_column)

    cagr_quintile = roic_obj.get_cagr_as_df()
    print(cagr_quintile)
