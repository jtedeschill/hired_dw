from faker import Faker
import pandas as pd
faker = Faker()
import logging
import yaml
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from pathlib import Path
from etl import ETL, EL
from etl.extractors import SalesforceExtractor
from etl.transformers import PandasTransformer
from etl.loaders import PandasGbqLoader, GcsLoader


logging.basicConfig(level=logging.INFO)




if __name__ == '__main__':

    salesforce_extractor = SalesforceExtractor()

    soql_query = "src/etl/queries/sfdc_account.sql"

    df = salesforce_extractor.extract(soql_query)

    logging.info(df.columns)

    # localize created date
    print(df['CreatedDate'].head())


    # localize to los angeles
    df['Test'] = pd.to_datetime(pd.to_datetime(df['Scraped_JD_Signal_Core_Tech__c']).dt.tz_convert('America/Los_Angeles').dt.strftime('%Y-%m-%d %H:%M:%S'))
    print(df[['Test','Scraped_JD_Signal_Core_Tech__c']].head())
    # convert to datetime



    

    
