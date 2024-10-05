import pandas as pd
import tabula


# to add a reference to the 'DatabaseConnector' class here, you need to pass an instance

class DataExtractor:
# i need to read the data from the RDS database

    # passing the databaseconnector as an instance
    # passing the linked_pdf as parameters
    def __init__(self,db_connector,linked_pdf):
        self.db_connector = db_connector #db_connector = instance of databse connector
        self.linked_pdf = linked_pdf
        # now read the data 

    # we'll use the connection engine to query the database
    # when reading the data we take the table_name as an argument
    def read_rds_table (self,table_name): 
        engine = self.db_connector.db_engine

        # CHECK IF TABLE EXISTS IN database
        available_tables = self.db_connector.list_db_tables()
        
        # we check if inputted table (the parameter) is in database
        if table_name not in available_tables:
            raise ValueError(f"Table {table_name} is not in database")

        #  now we are going to run queries
        query = f"SELECT * FROM {table_name}"
        # loading table data in pandas datafram
        df = pd.read_sql(query,engine)

        return df
    
    def retrieve_pdf_data(self):
        # We are using linked_pdf we dont pass it as an argument
        #  we want to extract from all the pages
        dfs = tabula.read_pdf(self.linked_pdf, pages="all" , multiple_tables = True)

        if not dfs :
            raise ValueError("No tables found in PDF")

#  we are combining the total number of rows
        return pd.concat(dfs,ignore_index=True)

