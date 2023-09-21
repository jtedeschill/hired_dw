from etl.extractors import SalesforceExtractor
from etl.transformers import SFTransformer
from etl.loaders import PandasGbqLoader
from etl import ETL
import yaml
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    with open('./src/tasks.yml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    etl = ETL()
    for task in config:
        logging.info(f'Running task {task["name"]}')
        etl.initialize(task['name'], SalesforceExtractor(), SFTransformer() , PandasGbqLoader(), task)
        etl.run()