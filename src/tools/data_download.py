from operator import inv


class DownloadDataFMP():

    def __init__(self, api_key, first_part_download_link, second_part_download_link, investment_universe_path, save_path):

        self.api_key = api_key
        self.first_part_download_link = first_part_download_link
        self.second_part_download_link = second_part_download_link
        self.investment_universe_path = investment_universe_path
        self.save_path = save_path

    def get_full_download_link(self):
        return self.first_part_download_link 
    
if __name__ == '__main__':
    import configparser
    import os
    import pandas as pd

    config = configparser.ConfigParser()

    config_file_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'config', 'config.ini'))
    config.read(config_file_path)

    API_KEY = config['FMP DATA']['api_key']
    FIRST_PART_DOWNLOAD_LINK = config['FMP DATA']['download_link_first_part']
    LIMIT = config['FMP DATA']['limit']
    second_part_download_link = "_ticker_?period=quarter&limit=_limit_&apikey=_apikey_"
    investment_universe_path = r"\data\investment universe.csv"
    print(pd.read_csv(investment_universe_path))




