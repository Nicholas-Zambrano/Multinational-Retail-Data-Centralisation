import pandas as pd
from data_extraction import DataExtractor

class DataCleaning:

    # we accept data extractor instance
    def __init__(self,data_extractor):
        self.data_extractor = data_extractor
    
    # we are going to call data extractor to read the legacy user table
    def clean_user_data(self):
        # calling the class and reading its method
        df = self.data_extractor.read_rds_table("legacy_users")
#       removing rows with null values
        df = df.dropna(subset = ["first_name","last_name","user_uuid"])
        # ensure date columns are valid, so convert to date type
        df["join_date"] = pd.to_datetime(df["join_date"],errors="coerce")

        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"],errors = "coerce")

        # those we couldn't convert to date type we drop it
        df.dropna(subset = ['join_date',"date_of_birth"], inplace = True)
        return df



