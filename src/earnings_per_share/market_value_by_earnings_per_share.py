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
                #              dataset = datapoints
                # unpacked = pd.DataFrame.from_records([{**d} for d in data])
                # import itertools as it
                # #unpacked = pd.DataFrame.from_records(it.chain.from_iterable(data))
                # print(unpacked)
                marketValue = pd.read_csv(marketValueLink, usecols = ['Ticker','MarketCap'])

                marketValue[['avgPctChange','coeffVariation']] = items
                #grouped = marketValue.sort_values(['MarketCap', 'avgPctChange'], ascending=[False, False])
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

                # marketCapGroupedTickers = [[num, list(split_sorted_split[num]['Ticker'])]
                #         for num in range(len(split_sorted_split))]

                marketCapGroupedTickers = [{num: list(split_sorted_split[num]['Ticker'])} for num in range(len(split_sorted_split))]


                # with open('/Users/adamszequi/SmartFactor/Smart-Factor-Research-Files-5/Market Value by Earnings Per Share/Datasets/marketCapGroupedTickersFile.txt', 'w') as file:
                #     file.write(json.dumps(marketCapGroupedTickers))

                return list_of_avg
                


mark = AnnualEPS()
# data = mark.get_eps_pctChange('/Users/adamszequi/SmartFactor/Smart-Factor-Research-Files-5/testcompanies2.csv'
#                    ,'Ticker','/Users/adamszequi/SmartFactor/Smart-Factor-Research-Files-5/ticker_eps_avgPctChange.txt')
#print(data)
EPS_file=mark.combiningEPSMarketValue\
  ('/Users/adamszequi/Desktop/Clones/EPS/data/Test Company Tickers , Earnings Per Share , Avg Percentage Change Dataframe.txt',
     '/Users/adamszequi/Desktop/Clones/EPS/data/Test Companies.csv')

print('Table of Market Value divided into Quintiles - Contents represent Average EPS of quintile portfolio')
print('----------------------------------------------------------------------------------------------------')
print(EPS_file.to_markdown())