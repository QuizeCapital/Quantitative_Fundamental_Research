import json
from pprint import pprint
from urllib.request import urlopen

import certifi
import numpy as np
import pandas as pd


class AnnualEPS(): 
        
        def __init__(self) -> None:
            pass
                
        def combiningEPSMarketValue(self,url,marketValueLink):
         
                items=[]
                keys=[]
                with open(url) as f:
                        data = json.load(f)
                        for datapoints in data:
                            for key,item in datapoints.items():
                                keys.append(key)
                                items.append(item)

                marketValue = pd.read_csv(marketValueLink, usecols = ['Ticker','MarketCap'])

                marketValue[['avgPctChange','coeffVariation']] = items
                grouped = marketValue.sort_values(['MarketCap'], ascending=[False,])

                splitByMarketValue= np.array_split(grouped, 5)
                split_sorted = [dataframe.sort_values('avgPctChange', ascending=False) for dataframe in splitByMarketValue]
                split_sorted_split = sum(
                    (np.array_split(dataframe, 5) for dataframe in split_sorted), [])

                list_of_limit_markCaps = [
                    '>' + "{:,}".format(df['MarketCap'].iloc[-1])
                    for df in splitByMarketValue
                ]
                list_of_avg = pd.DataFrame(np.array([cap['avgPctChange'].mean() for cap in split_sorted_split]).reshape(5,5),index=list_of_limit_markCaps)

                list_of_avg = list_of_avg.assign(Range=list_of_avg.values.max(1)-list_of_avg.values.min(1))
                list_of_avg['Average'] = list_of_avg.mean(axis=1)
                list_of_avg.loc['Average'] = list_of_avg.mean()


                marketCapGroupedTickers = [{num: list(split_sorted_split[num]['Ticker'])} for num in range(len(split_sorted_split))]


                return list_of_avg
                


mark = AnnualEPS()

EPS_file=mark.combiningEPSMarketValue\
  ('/Users/adamszequi/Desktop/Clones/EPS/data/Test Company Tickers , Earnings Per Share , Avg Percentage Change Dataframe.txt',
     '/Users/adamszequi/Desktop/Clones/EPS/data/Test Companies.csv')

print('Table of Market Value divided into Quintiles - Contents represent Average EPS of quintile portfolio')
print('----------------------------------------------------------------------------------------------------')
print(EPS_file.to_markdown())