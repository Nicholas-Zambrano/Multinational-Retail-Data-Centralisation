import pandas as pd
from data_extraction import DataExtractor
import numpy as np

class DataCleaning:

    # we accept data extractor instance
    def __init__(self,data_extractor,db_connector):
        self.data_extractor = data_extractor
        # adding database connector instance to updload 
        self.db_connector = db_connector


    
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

# we are using linked pdf from dataEctractor
    def clean_card_data(self):
        df_card = self.data_extractor.retrieve_pdf_data()

        # lets infer the data time 
        df_card["date_payment_confirmed"] = pd.to_datetime(df_card["date_payment_confirmed"], errors="coerce")
        # drop rows with null values:
        df_card= df_card.dropna(subset=["card_number","expiry_date","card_provider"])

        # check card number only contain numbers, so we convert it to string and use regex:
        df_card = df_card[df_card["card_number"].astype(str).str.match(r'^\d+$')]

        return df_card







