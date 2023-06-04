# import pandas as pd
# import dask.dataframe as dd
# import ast
# import json
# path = r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\historical_price.parquet'
# df  = pd.read_parquet(path)
# print('Data Read')
# df['historical'] = df['historical'].astype(str).apply(ast.literal_eval)
# print('Data Converted')
# df = pd.concat([df.drop(['historical'], axis=1), df['historical'].apply(pd.Series)], axis=1)
# print('Data Concatenated')

# df['date'] = pd.to_datetime(df['date'])
# print('Data Converted to Datetime')
# df['year'] = df['date'].dt.year
# print('Year Column Created')

# df = df.groupby('symbol').apply(lambda x: x.sort_values('date', ascending=True))
# print('Data Sorted')
# df = df.set_index('symbol')
# print('Data Indexed')
# df['daily_return'] = df.groupby(['symbol', 'year'])['close'].pct_change()
# print('Daily Return Calculated')

# yearly_pct_ch = df.groupby(['symbol', 'year'])['daily_return'].sum().mul(100).reset_index().rename(columns={'daily_return': 'cum_pct_ch_year'})
# print('Yearly Return Calculated')
# yearly_pct_ch.to_excel(r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\annual_historical_returns(sym_ticker,return).xlsx', index=False)
# print('Data Exported')
# os.startfile(r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\annual_historical_returns(sym_ticker,return).xlsx')


import pandas as pd
import ast

path = r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\historical_price.parquet'
df = pd.read_parquet(path)
print('Data Read')

df['historical'] = df['historical'].astype(str).apply(ast.literal_eval)
print('Data Converted')

# df = pd.concat([df.drop('historical', axis=1), df['historical'].apply(pd.Series)], axis=1)
df = pd.concat([df.drop('historical', axis=1), pd.json_normalize(df['historical'])], axis=1)

print('Data Concatenated')

df['year'] = pd.to_datetime(df['date']).dt.year
print('Data Converted to Datetime')

# df['year'] = df['date'].dt.year
# print('Year Column Created')

df.sort_values(['symbol', 'date'], ascending=True, inplace=True)
print('Data Sorted')

df.set_index('symbol', inplace=True)
print('Data Indexed')

df['daily_return'] = df.groupby(['symbol', 'year'])['close'].pct_change()
print('Daily Return Calculated')

yearly_pct_ch = (df.groupby(['symbol', 'year'])['daily_return'].sum() * 100).reset_index().rename(columns={'daily_return': 'cum_pct_ch_year'})
# yearly_pct_ch = yearly_pct_ch.reset_index().rename(columns={'daily_return': 'cum_pct_ch_year'})
# print('Yearly Return Calculated')

output_path = r'C:\Users\adamszeq\Desktop\.01\GitHub\Quantitative_Fundamental_Research\data\annual_historical_returns(sym_ticker,return).xlsx'
yearly_pct_ch.to_excel(output_path, index=False)
print('Data Exported')
