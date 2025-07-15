import sys 
import pandas as pd
import numpy as np
from typing import Optional

from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME
from src.exception import MyException

class Proj1Data:
    """
    This class handles all data-related operations for the Proj1 project,
    including fetching data from MongoDB and converting it into a Pandas DataFrame.
    """

    def __init__(self):
        """
        Initializes the Proj1Data class by establishing a connection to MongoDB.
        """
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        Exports a MongoDB collection as a Pandas DataFrame.

        Args:
            collection_name (str): The name of the collection to export.
            database_name (Optional[str]): The name of the database. If None, uses the default database.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the specified collection.

        Raises:
            MyException: If there is an error during the data export process.
        """
        try:
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client.client[database_name][collection_name]

            df = pd.DataFrame(list(collection.find()))
            
            if "_id" in df.columns:
                df = df.drop(columns=["_id"])
            df.replace({"na": np.nan},inplace=True)
            return df

        except Exception as e:
            raise MyException(e, sys)
