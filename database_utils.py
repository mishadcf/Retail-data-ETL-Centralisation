import yaml
import sqlalchemy


class DatabaseConnector:
    def read_db_creds(self, db_creds_file="db_creds.yaml", env="RDS"):
        """Gets information from the credentials .yaml file, RDS by default"""
        with open(db_creds_file, "r") as file:
            y = yaml.safe_load(file)
        return y.get(env)

    def init_db_engine(self, env="RDS"):
        """creates an sqlalchemy database engine from the YAML credentials, allowing for local environment and the RDS"""
        creds = self.read_db_creds(env=env)

        connection_string = f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}"
        engine = sqlalchemy.create_engine(connection_string)
        return engine

    def list_db_tables(self):
        """
        Retrieve a list of table names from the connected database.

        This method initializes a database connection using the stored
        credentials and retrieves the names of all tables present in the database.
        It utilizes SQLAlchemy to inspect the database metadata.

        Returns
        -------
        list of str
            Names of the tables in the connected database.

        Raises
        ------
        sqlalchemy.exc.SQLAlchemyError
            If any SQLAlchemy related errors occur during the execution.

        Example
        -------
        >>> db_connector = DatabaseConnector()
        >>> tables = db_connector.list_db_tables()
        >>> print(tables)
        ['table1', 'table2', 'table3']
        """
        engine = self.init_db_engine()
        with engine.connect():
            inspector = sqlalchemy.inspect(engine)
            table_names = inspector.get_table_names()
        return table_names

    def upload_to_db(self, table_name, dataframe, if_exists="fail", env="LOCAL"):
        """
        Uploads a DataFrame to the connected database.

        Parameters:
        - table_name (str): Name of the table to which data should be uploaded.
        - dataframe (pd.DataFrame): The dataframe to be uploaded.
        - if_exists (str): What to do if the table already exists. Options: 'fail', 'replace', 'append'. Default 'fail'.

        Returns:
        -------
        None
        """
        engine = self.init_db_engine(env="LOCAL")
        dataframe.to_sql(table_name, engine, index=False, if_exists=if_exists)
