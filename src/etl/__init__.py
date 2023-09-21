from abc import ABC, abstractmethod
import yaml
from pathlib import Path
import importlib
import logging
from registry  import register_class



class Task(ABC):
    @abstractmethod
    def run(self):
        pass

@register_class
class ETL(Task):
    def __init__(self):
        pass

    def initialize(self, name, extractor, transformer, loader, params):
        self.name = name
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.params = params

    def run(self):
        dataframe = self.extractor.extract(**self.params["extractor"]["params"])
        transformed_dataframe = self.transformer.transform(dataframe, **self.params["transformer"]["params"])
        self.loader.load(transformed_dataframe, **self.params["loader"]["params"])

@register_class
class EL(Task):
    def __init__(self):
        pass
    
    def initialize(self, name, extractor, loader, params):
        self.name = name
        self.extractor = extractor
        self.loader = loader
        self.params = params

    def run(self):
        dataframe = self.extractor.extract(**self.params["extractor"]["params"])
        self.loader.load(dataframe, **self.params["loader"]["params"])