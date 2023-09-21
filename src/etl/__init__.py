from abc import ABC, abstractmethod
import yaml
from pathlib import Path
import importlib
import logging


def register_class(cls):
    cls.is_registered = True
    return cls

def load_registry(module_name : str|list[str]):
    if isinstance(module_name, str):
        module_name = [module_name]
    
    registry = {}
    for module in module_name:
        module = importlib.import_module(module)
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)
            if isinstance(attribute, type) and getattr(attribute, "is_registered", False):
                registry[attribute_name] = attribute
    return registry

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


def execute_tasks(file_path : str|Path):
    if isinstance(file_path, str):
        file_path = Path(file_path)
    with open(file_path, 'r') as file:
        tasks = yaml.safe_load(file)
    registry = load_registry(['src.etl.loaders', 'src.etl.extractors', 'src.etl.transformers', 'src.etl'])
    logging.info(f'Loaded {len(tasks)} tasks')
    for task in tasks:
        logging.info(f'Running task {task["name"]}')
        data_task = registry[task["type"]]
        data_task.initialize(name = task["name"], extractor = registry[task["extractor"]["class"]], loader = registry[task["loader"]["class"]], params = task)
        data_task.run()
        logging.info(f'Finished task {task["name"]}')

