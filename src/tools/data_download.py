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

    config = configparser.ConfigParser()
    config.read('../config/config.ini')

    # check if config file is correct
    print('The config file is correct:', config.sections() == ['FMP Data'])