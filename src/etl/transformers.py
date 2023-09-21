from abc import ABC, abstractmethod
import pandas as pd
import logging
import yaml
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from registry import register_class
import json
from pathlib import Path

class Transformer(ABC):
    @abstractmethod
    def transform(self):
        pass


@register_class
class PandasTransformer(Transformer):
    """ Simple transformer that transforms a dataframe through a function """
    def __init__(self, dataframe, function):
        self.dataframe = dataframe
        self.function = function

    def transform(self):
        logging.info(f'Transforming {len(self.dataframe)} records')
        return self.function(self.dataframe)

@register_class
class SFTransformer(Transformer):

    def __init__(self):
        pass

    def transform(self, dataframe, schema: str|None = None, remove_suffix=True, lowercase=True):
        """ Transform a dataframe from Salesforce 
        To apply a schema, pass a path with a json with the following format:
        ```
        {'column_name': ('data_type','final column name')}
        ```
        - The data type can be any of the following: 'date', 'int64', 'float64', 'string'
        - The final column name is the name of the column after the transformation
        - Any columns not included in the schema will be dropped

        Example Schema:
        ```
        {
            'id': ('string', 'id'),
            'name': ('string', 'name'),
            'address': ('string', 'address'),
            'email': ('string', 'email'),
            'phone_number': ('string', 'phone_number'),
            'job': ('string', 'job'),
            'company': ('string', 'company'),
            'status': ('string', 'status'),
            'createddate': ('date', 'created_date'),
        }
        ```

        If a schema is not passed, the following transformations will be applied:
        - Remove the __c suffix from the column names
        - Remove the __r suffix from the column names
        - Trim dunder from column names
        - Convert all column names to lowercase


        """
        self.dataframe = dataframe
        logging.info(f'Transforming {len(self.dataframe)} records')

        if schema:
            with open(Path(schema)) as f:
                schema = json.loads(f.read())
        else:
            schema = None

        if schema:
            # drop columns that are not in the schema
            self.dataframe = self.dataframe[schema.keys()]
            # apply data type conversions
            for column_name, (data_type, _) in schema.items():
                # if it is a date, convert to datetime
                if data_type == 'date':
                    self.dataframe[column_name] = pd.to_datetime(self.dataframe[column_name])
                # otherwise, convert to the specified data type
                else:
                    self.dataframe[column_name] = self.dataframe[column_name].astype(data_type)
            # apply column name conversions
            for column_name, (_, final_column_name) in schema.items():
                self.dataframe = self.dataframe.rename(columns={column_name: final_column_name})



        elif remove_suffix:
            dataframe = (dataframe
                        # Remove the __c suffix from the column names
                        .rename(columns=lambda x: x.replace('__c', ''))
                        # Remove the __r suffix from the column names
                        .rename(columns=lambda x: x.replace('__r', ''))
                        # Trim dunder from column names
                        .rename(columns=lambda x: x.strip('_'))
            )
        if lowercase:
            dataframe = dataframe.rename(columns=lambda x: x.lower())
        return self.dataframe