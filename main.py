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
    # we
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

    extracted_data = DataExtractor(db_connector,pdf_link)

    # now we clean the data, so we call the DataCleaning class
    cleaner = DataCleaning(extracted_data,db_connector)

    # we using its method within the class
    print("Cleaning user data...")

    cleaned_user_data = cleaner.clean_user_data()

    db_connector.upload_to_db(cleaned_user_data, 'dim_users')
    print("Data successfully uploaded to local database!")

    print("cleaning card details")
    # passing the pdf link, and calling methods from cleaning class
    cleaned_card_details = cleaner.clean_card_data()

    db_connector.upload_to_db(cleaned_card_details,'dim_card_details')
    

if __name__== "__main__":
    main()

