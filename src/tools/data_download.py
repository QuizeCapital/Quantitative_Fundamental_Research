# Download data from FMP API
import json
from operator import inv
from urllib.request import urlopen

import certifi
import pandas as pd


class DownloadDataFMP():

    def __init__(self, api_key, first_part_download_link, second_part_download_link,
                investment_universe_path, investment_universe_ticker, save_path, limit, cols_to_download,
                download_all_columns):

        self.api_key = api_key
        self.first_part_download_link = first_part_download_link
        self.second_part_download_link = second_part_download_link
        self.investment_universe_path = investment_universe_path
        self.save_path = save_path
        self.investment_universe_ticker = investment_universe_ticker
        self.limit = limit
        self.cols_to_download = cols_to_download
        self.download_all_columns = download_all_columns

    def read_investment_universe(self):
        return pd.read_csv(self.investment_universe_path)

    def get_full_download_link(self, ticker):

        self.ticker = ticker
        second_part_download_link = self.second_part_download_link.replace(
            '_ticker_', self.ticker) .replace('_limit_', LIMIT).replace('_apikey_', API_KEY)

        return self.first_part_download_link + second_part_download_link

    def download_data(self):
        data_list = []  

        if self.download_all_columns == True:
            print('Downloading all columns...')
            for count, ticker in enumerate(self.read_investment_universe()[self.investment_universe_ticker]):
                full_link = self.get_full_download_link(ticker)

                response = urlopen(full_link, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if data := json.loads(data):
                    print('Downloading data:', count, 'for:', ticker)
                    data_list.append(data)  

        else:
            print('Downloading selected columns...')
            for count, ticker in enumerate(self.read_investment_universe()[self.investment_universe_ticker]):
                full_link = self.get_full_download_link(ticker)

                response = urlopen(full_link, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if data := json.loads(data):
                    print('Downloading data:', count, 'for:', ticker)
                    selected_data = {key: value for key, value in data.items() if key in self.cols_to_download}
                    data_list.append(selected_data)  

        return pd.DataFrame(data_list)


    
    def save_data(self):

        df = self.download_data()
        df.to_excel(self.save_path, index=False)

        return df


if __name__ == '__main__':
    import configparser
    import os

    import pandas as pd

    config = configparser.ConfigParser()

    config_file_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'config', 'config.ini'))
    config.read(config_file_path)

    API_KEY = config['FMP DATA']['api_key']
    FIRST_PART_DOWNLOAD_LINK = eval(
        config['FMP DATA']['download_link_first_part'])[0]
    LIMIT = config['FMP DATA']['limit']
    INVESTMENT_UNIVERSE_TICKER_COLUMN = config['FMP DATA']['investment_universe_ticker_column']
    cols_to_download = ['symbol', 'date', 'roic', 'priceToSalesRatio']

    second_part_download_link = "key-metrics/_ticker_?&limit=_limit_&apikey=_apikey_"
    investment_universe_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'investment universe.csv'))
    save_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'roic_pricetobookvalue_data.xlsx'))

    download_data = DownloadDataFMP(API_KEY, FIRST_PART_DOWNLOAD_LINK, second_part_download_link, investment_universe_path,
                                    INVESTMENT_UNIVERSE_TICKER_COLUMN, save_path, LIMIT, cols_to_download, True)
    print(download_data.save_data())
