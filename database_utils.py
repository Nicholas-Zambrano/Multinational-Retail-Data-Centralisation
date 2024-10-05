import yaml
import sqlalchemy
import psycopg2

class DatabaseConnector:

    # so the input will be credentials, so we passed a parameter to handle the input i.e 'creds'
    def __init__(self,creds_file):
        
        # assigned variable 'creds_file' to parameter
        self.creds_file = creds_file
        
        self.creds = self.read_db_creds() # here we call the 'read_db_creds' method to read and return the credentials, and stored it in a variable called creds
        self.db_engine = self.init_db_engine() # we stored the enginer

    # we will be reading the yaml file
    def read_db_creds(self):

        try:
        # reading the file passed as paramter, saved as variable 'file'
            with open(self.creds_file,'r') as file:
                creds = yaml.safe_load(file)
            
            # returning the file , which saved as 'creds'
            return creds
        except Exception as e:
            print(f"error reading credentials: {e}")
            raise
            

    # initializing databse connection
    def init_db_engine(self):

        # we call the read_db_creds method and store it in a variable - this reads database from yaml file
        # we need this to build the connection string
        creds = self.read_db_creds()
        # now we need to connect to the database, and sql achemy requires a connection string to connect to the database, 
        # we make a connection string using creds

        database_type = "postgresql"
        username = creds['RDS_USER']
        password = creds['RDS_PASSWORD']
        host = creds['RDS_HOST']
        port = creds['RDS_PORT']
        database = creds['RDS_DATABASE']

        connection_string =  f"{database_type}://{username}:{password}@{host}:{port}/{database}"

        # here we create a SQLAlchemy engine ,so it takes the connection string as an argument and returns an engine
        engine = sqlalchemy.create_engine(connection_string)
        return engine
    
    def list_db_tables(self):
        # we will use the engine to inspect the database tables
        engine = self.db_engine
        with engine.connect() as connection:
            metadata = sqlalchemy.MetaData()
            metadata.reflect(bind= connection)
            table_names = metadata.tables.keys()
        return table_names


    # uploading to our new local database
    def upload_to_db(self,df,table_name):

        creds = self.read_db_creds()

        # we need to create a new database connection
        local_db_type = 'postgresql'
        local_db_api = 'psycopg2'
        local_host = 'localhost' #database is runnnig on local machine
        local_user = creds['Local']['LOCAL_USER'] #username used to connect to database
        local_password = creds['Local']['LOCAL_PASSWORD']
        local_db = creds['Local']['LOCAL_DB']
        local_port = creds['Local']['LOCAL_PORT']

        # create SQLAlchemy engine
        local_engine = sqlalchemy.create_engine(f"{local_db_type}+{local_db_api}://{local_user}:{local_password}@{local_host}:{local_port}/{local_db}")

        df.to_sql(table_name,local_engine, if_exists = 'replace')





