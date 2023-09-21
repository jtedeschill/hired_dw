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
from abc import ABC, abstractmethod
from registry import register_class
from simple_salesforce import Salesforce, SalesforceLogin, SFType

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

@register_class
class FakeDataExtractor(Extractor):
    """ Generate fake data, for testing purposes """

    def __init__(self):
        pass

    def extract(self, num_records):
        """ Generate a pandas dataframe with the following columns: 
        id, name, address, email, phone_number, job, company, status
        """
        self.num_records = num_records
        fake_data = []
        logging.info(f'Generating {self.num_records} fake records')
        for _ in range(self.num_records):
            fake_data.append({
                'id': faker.uuid4(),
                'name': faker.name(),
                'address': faker.address(),
                'email': faker.email(),
                'phone_number': faker.phone_number(),
                'job': faker.job(),
                'company': faker.company(),
                'status': faker.random_element(elements=('active', 'inactive', 'pending')),
            })
        dataframe = pd.DataFrame(fake_data)
        logging.info(f'Generated {len(dataframe)} fake records')
        return dataframe

@register_class  
class SalesforceExtractor(Extractor):
    """ Extract data from Salesforce """
    def __init__(self):
        pass


    def extract(self, soql_query) -> pd.DataFrame:
        """ Extract data from Salesforce using a SOQL query """
        user = os.environ.get('SF_USER')
        password = os.environ.get('SF_PASSWORD')
        token = os.environ.get('SF_TOKEN')
        self.sf = Salesforce(username=user, password=password, security_token=token, version='52.0')
        logging.info(f'Connected to Salesforce as {user}')
        logging.info(f'Extracting data from Salesforce using query: {soql_query}')
        records = self.sf.query_all(soql_query)['records']
        logging.info(f'Extracted {len(records)} records from Salesforce')
        dataframe = pd.json_normalize(records, sep='_')

        return dataframe