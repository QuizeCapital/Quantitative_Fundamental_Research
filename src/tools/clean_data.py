class CleanData():
    
    def __init__(self, data, date_column, frequency, needed_columns):
        self.data = data
        self.date_column = date_column
        self.needed_columns = needed_columns
        self.frequency = frequency

    