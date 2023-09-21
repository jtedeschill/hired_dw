from abc import ABC, abstractmethod
import pandas as pd
import logging
import yaml
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from registry import register_class

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