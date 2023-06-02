from datetime import date
import json
from operator import inv
from urllib.request import urlopen

import certifi
import pandas as pd


class DownloadDataFMP():

    def __init__(self, api_key, first_part_download_link, second_part_download_link,
                investment_universe_path, investment_universe_ticker, save_path, limit):

        self.api_key = api_key
        self.first_part_download_link = first_part_download_link
        self.second_part_download_link = second_part_download_link
        self.investment_universe_path = investment_universe_path
        self.save_path = save_path
        self.investment_universe_ticker = investment_universe_ticker
        self.limit = limit

    def read_investment_universe(self):
        return pd.read_csv(self.investment_universe_path)

    def get_full_download_link(self, ticker):

        self.ticker = ticker
        second_part_download_link = self.second_part_download_link.replace(
            '_ticker_', self.ticker) .replace('_limit_', LIMIT).replace('_apikey_', API_KEY)

        return self.first_part_download_link + second_part_download_link

    def download_data(self):

        for ticker in self.read_investment_universe()[self.investment_universe_ticker]:
            full_link = self.get_full_download_link(ticker)

            response = urlopen(full_link, cafile=certifi.where())
            data = response.read().decode("utf-8")
            if data := json.loads(data):

                symbol, date, roic, priceToSalesRatio = data[0]['symbol'], data[0]['date'], data[0]['roic'], data[0]['priceToSalesRatio']
                
                


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

    second_part_download_link = "key-metrics/_ticker_?&limit=_limit_&apikey=_apikey_"
    investment_universe_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'investment universe.csv'))
    save_path = os.path.abspath(os.path.join(
        __file__, '..', '..', '..', 'data', 'roic_pricetobookvalue_data.csv'))

    download_data = DownloadDataFMP(API_KEY, FIRST_PART_DOWNLOAD_LINK, second_part_download_link, investment_universe_path,
                                    INVESTMENT_UNIVERSE_TICKER_COLUMN, save_path, LIMIT)
    print(download_data.download_data())
