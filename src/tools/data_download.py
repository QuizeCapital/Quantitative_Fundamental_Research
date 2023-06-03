# Download data from FMP API
import json
from operator import inv
from urllib.request import urlopen

import certifi
import pandas as pd


class DownloadDataFMP():

    def __init__(self, first_part_download_link, second_part_download_link,
                investment_universe_path, investment_universe_ticker, save_path, cols_to_download,
                download_all_columns, save_file_type, data_type):

        self.first_part_download_link = first_part_download_link
        self.second_part_download_link = second_part_download_link
        self.investment_universe_path = investment_universe_path
        self.save_path = save_path
        self.investment_universe_ticker = investment_universe_ticker
        self.cols_to_download = cols_to_download
        self.download_all_columns = download_all_columns
        self.save_file_type = save_file_type
        self.data_type = data_type

    def read_investment_universe(self):
        return pd.read_csv(self.investment_universe_path).head(10)
    
    def get_full_download_link(self, ticker):

        self.ticker = ticker

        if self.data_type == 'index':
            second_part_download_link = self.second_part_download_link

        elif self.data_type == 'ticker':
            second_part_download_link = self.second_part_download_link.replace(
                '_ticker_', self.ticker) 

        return self.first_part_download_link + second_part_download_link

    def download_ticker_data(self):

        df = pd.DataFrame()

        if self.download_all_columns == True:
            print('Downloading all columns...')
            for count, ticker in enumerate(self.read_investment_universe()[self.investment_universe_ticker]):
                full_link = self.get_full_download_link(ticker)

                response = urlopen(full_link, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if data := json.loads(data):
                    print('Downloading data: ',count, 'for:', ticker)
                    
                    df = pd.concat([df, pd.DataFrame(data)])

        else:
            print('Downloading selected columns...')
            for count, ticker in enumerate(self.read_investment_universe()[self.investment_universe_ticker]):
                full_link = self.get_full_download_link(ticker)

                response = urlopen(full_link, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if data := json.loads(data):
                    print('Downloading data: ',count, 'for:', ticker)
                    
                    df = pd.concat([df, pd.DataFrame(data)[self.cols_to_download]])

        return df

    def download_index_data(self):

        df = pd.DataFrame()

        if self.download_all_columns == True:
            print('Downloading all columns...')
            full_link = self.get_full_download_link(ticker)

            response = urlopen(full_link, cafile=certifi.where())
            data = response.read().decode("utf-8")
            if data := json.loads(data):
                df = pd.concat([df, pd.DataFrame(data)])

        else:
            print('Downloading selected columns...')
            full_link = self.get_full_download_link(ticker)

            response = urlopen(full_link, cafile=certifi.where())
            data = response.read().decode("utf-8")
            if data := json.loads(data):  
                df = pd.concat([df, pd.DataFrame(data)[self.cols_to_download]])

        return df
    def save_data(self):

        if self.save_file_type == 'excel':
            print('Saving data to excel...')
            df = self.download_ticker_data()
            df.to_excel(self.save_path, index=False)

        elif self.save_file_type == 'parquet':
            print('Saving data to parquet...')
            df = self.download_ticker_data()
            df.to_parquet(self.save_path, index=False)

        return 'Data saved!'


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

    second_part_download_link = "historical-price-full/_ticker_?apikey=_apikey_"
    second_part_download_link = second_part_download_link.replace('_apikey_', API_KEY)
    investment_universe_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'investment universe.csv'))
    save_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'S&P_historical_price.parquet'))

    download_data = DownloadDataFMP(FIRST_PART_DOWNLOAD_LINK, second_part_download_link, investment_universe_path,
                                    INVESTMENT_UNIVERSE_TICKER_COLUMN, save_path, cols_to_download, True, 'parquet', 'index')
    print(download_data.save_data())
