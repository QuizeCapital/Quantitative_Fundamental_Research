import pandas as pd
import dask.dataframe as dd
import ast
import json
path = r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\historical_price.parquet'
df  = pd.read_parquet(path)

df['historical'] = df['historical'].astype(str).apply(ast.literal_eval)

df = pd.concat([df.drop(['historical'], axis=1), df['historical'].apply(pd.Series)], axis=1)

df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year

df = df.groupby('symbol').apply(lambda x: x.sort_values('date', ascending=True))
df = df.set_index('symbol')
df['daily_return'] = df.groupby(['symbol', 'year'])['close'].pct_change()

yearly_pct_ch = df.groupby(['symbol', 'year'])['daily_return'].sum().mul(100).reset_index().rename(columns={'daily_return': 'cum_pct_ch_year'})

yearly_pct_ch.to_excel(r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\annual_historical_returns(sym_ticker,return).xlsx', index=False)
