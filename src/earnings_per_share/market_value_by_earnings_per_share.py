import pandas as pd
from urllib.request import urlopen
import certifi
import json
import numpy as np
from pprint import pprint

class AnnualEPS(): 
        
        def __init__(self) -> None:
              pass
          
        def get_eps(self,datasets,ticker_column,ticker_data):
            data = pd.read_csv(datasets)
            eps_dict_list=[]
            for ticker_id in data[ticker_column]:
                url = f'https://financialmodelingprep.com/api/v3/income-statement/{ticker_id}?&apikey=764a0c82850f17c8235116b78792d7e1'
                response = urlopen(url, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if earnings_info_data := json.loads(data):
                    earnings_info = earnings_info_data
                period=(1/len(earnings_info)) if len(earnings_info) != 0 else 0
                first_info=earnings_info[0]
                first_eps=first_info.get('epsdiluted')
                last_info=earnings_info[-1]
                last_eps=last_info.get('epsdiluted')
                eps_growth_diff = (first_eps/last_eps) if last_eps != 0 else 0
                if eps_growth_diff < 0:
                   eps_growth_diff_time= -(abs(eps_growth_diff)**period)
                else:
                   eps_growth_diff_time= (eps_growth_diff**period)
                eps_growth_diff_time_minus_one=eps_growth_diff_time-1
                eps_growth_rate=eps_growth_diff_time_minus_one
                eps_data = {ticker_id: eps_growth_rate * 100}
                eps_dict_list.append(eps_data)

        def get_eps_pctChange(self,datasets,ticker_column,ticker_data):
                data = pd.read_csv(datasets)
                eps_dict_list = []
                eps_figures = []
                for ticker_id in data[ticker_column]:
                        url = f'https://financialmodelingprep.com/api/v3/income-statement/{ticker_id}?&apikey=764a0c82850f17c8235116b78792d7e1'
                        response = urlopen(url, cafile=certifi.where())
                        data = response.read().decode("utf-8")
                        if earnings_info_data := json.loads(data):
                            earnings_info_bulk = earnings_info_data
                        eps=np.array([data ['epsdiluted'] for data in earnings_info_bulk])

                        eps_quantiled = eps[(eps>np.quantile(eps,0.05)) & (eps<np.quantile(eps,0.95))].tolist()

                        eps_quantiled.reverse()

                        eps_change = (pd.Series(eps_quantiled).pct_change().replace([np.inf, -np.inf], np.nan).mean())*100
                        eps_coeffVar = abs(np.std(eps_quantiled))/ abs(np.mean(eps_quantiled))
                        eps_data = {ticker_id: (eps_change , eps_coeffVar)}
                        eps_dict_list.append(eps_data)
                        # print(eps)
                        # #print(count)
                        # print(ticker_id , eps_change)
                        #print(eps_quantiled)
                
                # with open(ticker_data, 'w') as file:
                #    file.write(json.dumps(eps_dict_list))

                
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