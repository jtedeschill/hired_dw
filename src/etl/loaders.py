from abc import ABC, abstractmethod
import pandas as pd
import logging
import yaml
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from pathlib import Path
from registry import register_class


class Loader(ABC):
    @abstractmethod
    def load(self):
        pass

@register_class
class PandasGbqLoader(Loader):
    """ Load a pandas dataframe to BigQuery through the pandas-gbq library """
    def __init__(self):
        pass

    def load(self, dataframe, dataset_id, table_name, project_id, if_exists):
        self.dataframe = dataframe
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.project_id = project_id
        self.if_exists = if_exists

        logging.info(f'Loading {len(self.dataframe)} records to table {self.table_name} in BigQuery')
        self.dataframe.to_gbq(f'{self.dataset_id}.{self.table_name}', 
            project_id = self.project_id,
            if_exists = self.if_exists
        )
        logging.info(f'Loaded {len(self.dataframe)} records to table {self.project_id}.{self.dataset_id}.{self.table_name} in BigQuery')

@register_class
class GcsLoader(Loader):
    """ Load files to GCS """
    def __init__(self):
        pass

    def load(self, dataframe, bucket_name, filename):
        self.dataframe = dataframe
        self.bucket_name = bucket_name
        self.filename = filename
        logging.info(f'Uploading {self.filename} to GCS bucket {self.bucket_name}')
        self.dataframe.to_parquet(self.filename)
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.filename)
        blob.upload_from_filename(self.filename)