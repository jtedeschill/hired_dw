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
from etl import register_class

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
def SalesforceExtractor(Extractor):
    """ Extract data from Salesforce """
    def __init__(self, user, password, token):
        self.user = user
        self.password = password
        self.token = token

    def extract(self):
        pass