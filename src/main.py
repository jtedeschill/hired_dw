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
from etl.loaders import PandasGbqLoader, GcsLoader
from etl.extractors import FakeDataExtractor
from etl.transformers import PandasTransformer
from etl.config import load_ingestions_config, ETL


if __name__ == '__main__':

    # write your code here