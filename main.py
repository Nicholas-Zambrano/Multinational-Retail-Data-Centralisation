# now combining all the scripts
# we import the class - DataExtractor from the data_extraction.py
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning


def main():

    # now we initialise the database with its credentials
    # we call database connector and it takes a credential file
    print("hey")
    db_connector = DatabaseConnector("db_creds.yaml")

    # now extract data from legacy_users tables useing database extractor class
    extracted_data = DataExtractor(db_connector)

    # now we clean the data, so we call the DataCleaning class
    cleaner = DataCleaning(extracted_data)

    # we using its method within the class
    print("Cleaning user data...")

    cleaned_user_data = cleaner.clean_user_data()

    db_connector.upload_to_db(cleaned_user_data, 'dim_users')
    print("Data successfully uploaded to local database!")

if __name__== "__main__":
    main()

