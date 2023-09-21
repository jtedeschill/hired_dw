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
from etl.extractors import FakeDataExtractor
from etl.transformers import PandasTransformer
from etl.loaders import PandasGbqLoader, GcsLoader


logging.basicConfig(level=logging.INFO)




if __name__ == '__main__':
    with open('tests/test_tasks.yml') as f:
        tasks = yaml.safe_load(f)

    test_el = EL()
    test_el.initialize(tasks[0]['name'],FakeDataExtractor(), PandasGbqLoader(), tasks[0])
    test_el.run()


# {'task': None, 'name': 'raw_mock_data', 'class': 'EL', 'extractor': {'class': 'FakeDataExtractor', 'params': {'num_records': 1000}}, 'loader': {'class': 'PandasGbqLoader', 'params': {'table_name': 'raw_mock_data', 'dataset_name': 'sample', 'project_id': 'dbtlab-371120', 'if_exists': 'replace'}}}