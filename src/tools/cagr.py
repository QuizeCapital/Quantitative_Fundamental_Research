print('Calculating CAGR...')
import os
import re

import pandas as pd
from adhoc_tools import AdhocTools

class CAGR:
    """
    Class for calculating Compound Annual Growth Rate (CAGR) of a DataFrame.

    Inputs:
    - data_df (pd.DataFrame): DataFrame with date, symbol, and annual return columns
    - date_column (str): Name of the column in the DataFrame representing dates
    - return_column (str): Name of the column in the DataFrame representing annual returns
    - symbol_column (str): Name of the column in the DataFrame representing symbols

    Returns CAGR values in decimal format. Dates should be on a yearly basis.

    Output:
    - DataFrame with symbol and cagr columns
    """

    def __init__(self, data_df, date_column, return_column, symbol_column):
        self.data_df = data_df
        self.date_column = date_column
        self.return_column = return_column
        self.symbol_column = symbol_column

    def count_years_each_symbol(self):
        """
        Count the number of years for each symbol in the DataFrame.

        Returns:
        - df (pd.DataFrame): DataFrame with symbol and num_years columns
        """
        df = self.data_df
        df = df.groupby(self.symbol_column).count().reset_index()
        df = df.rename(columns={self.date_column: 'num_years'})
        df = df[[self.symbol_column, 'num_years']]
        return df
    
    def calculate_linked_returns(self):
        """
        Calculate the linked returns for each symbol in the DataFrame.

        Returns:
        - df (pd.DataFrame): DataFrame with symbol and linked returns columns
        """
        df = self.data_df
        df[self.return_column] = 1 + (df[self.return_column] / 100)
        df = df.groupby(self.symbol_column).apply(lambda x: x.sort_values(self.date_column, ascending=True)).reset_index(drop=True)
        df[self.return_column] = df.groupby(self.symbol_column)[self.return_column].cumprod()
        df = df.groupby(self.symbol_column).tail(1)
        df[self.return_column] = df[self.return_column] - 1

        return df
    
    def calculate_cagr(self):
        """
        Calculate the CAGR for each symbol in the DataFrame.

        Returns:
        - df (pd.DataFrame): DataFrame with symbol, num_years, and cagr columns
        """
        df_returns = self.calculate_linked_returns()
        df_years = self.count_years_each_symbol()

        df = pd.merge(df_returns, df_years, on=self.symbol_column, how='left')
        df['cagr'] = (df[self.return_column] ** (1 / df['num_years'])) - 1
        
        return df
    
    def final_df(self):
        """
        Generate the final DataFrame with symbol and cagr columns.

        Returns:
        - df (pd.DataFrame): DataFrame with symbol and cagr columns
        """
        df = self.calculate_cagr()
        df = df[[self.symbol_column, 'cagr']]
        return df


if __name__ == "__main__":
    from adhoc_tools import AdhocTools

    path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'annual_historical_returns(sym_ticker,return).xlsx'))
    
    df = AdhocTools(path).read_data()
    df['year'] = pd.to_datetime(df['year'])
    df = df.rename(columns={'year': 'date'})
    df = df.rename(columns={'cum_pct_ch_year': 'annual_return'})
    df = df[['date', 'symbol', 'annual_return']]
    
    date_column = 'date'
    symbol_column = 'symbol'
    return_column = 'annual_return'
        
    
    df = CAGR(df, date_column, return_column, symbol_column).final_df()
    print(df.head(100).to_markdown())
