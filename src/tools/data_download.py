# Download data from FMP API
import json
from operator import inv
from urllib.request import urlopen

import certifi
import pandas as pd


class DownloadTickerDataFMP():

    def __init__(self, first_part_download_link, second_part_download_link,
                investment_universe_path, investment_universe_ticker, save_path, cols_to_download,
                download_all_columns, save_file_type):

        self.first_part_download_link = first_part_download_link
        self.second_part_download_link = second_part_download_link
        self.investment_universe_path = investment_universe_path
        self.save_path = save_path
        self.investment_universe_ticker = investment_universe_ticker
        self.cols_to_download = cols_to_download
        self.download_all_columns = download_all_columns
        self.save_file_type = save_file_type

    def read_investment_universe(self):
        return pd.read_csv(self.investment_universe_path).head(10)
    
    def get_full_download_link(self, ticker):

        self.ticker = ticker

        print('Downloading Ticker Data...')
        second_part_download_link = self.second_part_download_link.replace(
            '_ticker_', self.ticker) 

        return self.first_part_download_link + second_part_download_link

    def download_ticker_data(self):

        df = pd.DataFrame()

        if self.download_all_columns == True:
            print('Downloading All columns...') 
            for count, ticker in enumerate(self.read_investment_universe()[self.investment_universe_ticker]):
                full_link = self.get_full_download_link(ticker)

                response = urlopen(full_link, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if data := json.loads(data):
                    print('Downloading data: ',count, 'for:', ticker)

                    df = pd.concat([df, pd.DataFrame(data)])

        else:
            print('Downloading Selected columns...')
            for count, ticker in enumerate(self.read_investment_universe()[self.investment_universe_ticker]):
                full_link = self.get_full_download_link(ticker)

                response = urlopen(full_link, cafile=certifi.where())
                data = response.read().decode("utf-8")
                if data := json.loads(data):
                    print('Downloading data: ',count, 'for:', ticker)

                    df = pd.concat([df, pd.DataFrame(data)[self.cols_to_download]])

        return df

    
    def save_data(self):
        df = self.download_ticker_data()
                    
        if self.save_file_type == 'excel':
            print('Saving data to excel...')
            df.to_excel(self.save_path, index=False)

        elif self.save_file_type == 'parquet':
            print('Saving data to parquet...')
            df.to_parquet(self.save_path, index=False)

        return 'Data saved!'
    

class DownloadIndexDataFMP():

        def __init__(self, first_part_download_link, second_part_download_link,
                investment_universe_path, investment_universe_ticker, save_path, cols_to_download,
                download_all_columns, save_file_type, index_name):

            self.first_part_download_link = first_part_download_link
            self.second_part_download_link = second_part_download_link
            self.investment_universe_path = investment_universe_path
            self.save_path = save_path
            self.investment_universe_ticker = investment_universe_ticker
            self.cols_to_download = cols_to_download
            self.download_all_columns = download_all_columns
            self.save_file_type = save_file_type
            self.index_name = index_name
        
        def read_investment_universe(self):
            return pd.read_csv(self.investment_universe_path).head(10)
    
        def get_full_download_link(self, ticker):

            self.ticker = ticker

            print('Downloading Index Data...')
            second_part_download_link = self.second_part_download_link.replace(
                '_ticker_', self.ticker) 

            return self.first_part_download_link + second_part_download_link


        def download_index_data(self):

            ticker = self.index_name

            df = pd.DataFrame()

            if self.download_all_columns == True:
                data = self._extracted_from_download_index_data_8(
                    'Downloading all columns for Index Data...', ticker
                )
                if data := json.loads(data):
                    df = pd.concat([df, pd.DataFrame(data)])

            else:
                data = self._extracted_from_download_index_data_8(
                    'Downloading Selected columns for Index Data...', ticker
                )
                if data := json.loads(data):  
                    df = pd.concat([df, pd.DataFrame(data)[self.cols_to_download]])

            return df

        def _extracted_from_download_index_data_8(self, arg0, ticker):
            print('Downloading Data...')
            print(arg0)
            full_link = self.get_full_download_link(ticker)
            print(full_link)

            response = urlopen(full_link, cafile=certifi.where())
            return response.read().decode("utf-8")
        
        def save_data(self):
            df = self.download_index_data()

            if self.save_file_type == 'excel':
                print('Saving data to excel...')
                df.to_excel(self.save_path, index=False)

            elif self.save_file_type == 'parquet':
                print('Saving data to parquet...')
                df.to_parquet(self.save_path, index=False)

            return 'Data saved!'



if __name__ == '__main__':
    #TODO FOR INDEX DATA
    #TODO change name of save_path to the neame needed
    #TODO change type of data to 1.download
    #TODO change index_name to the index name needed
    #TODO change cols_to_download to the columns needed
    #TODO change download_all_columns to True or False
    #TODO change save_file_type to excel or parquet
    #TODO change investment_universe_ticker_column to the column name of the ticker
    #TODO change investment_universe_path to the path of the investment universe
    #TODO change second_part_download_link to the second part of the download link

    #TODO FOR TICKER DATA
    #TODO change name of save_path to the neame needed
    #TODO change type of data to 1.download
    #TODO change cols_to_download to the columns needed
    #TODO change download_all_columns to True or False
    #TODO change save_file_type to excel or parquet
    #TODO change investment_universe_ticker_column to the column name of the ticker


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

    second_part_download_link = "historical-price-full/_ticker_?serietype=line&apikey=_apikey_"
    second_part_download_link = second_part_download_link.replace('_apikey_', API_KEY)
    investment_universe_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'investment universe.csv'))
    
    #TODO change name of save_path to the neame needed
    save_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', '_historical_price.xlsx'))
    

    #TODO change type of data to 1.download
    