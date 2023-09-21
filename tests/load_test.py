from src.etl.loaders import PandasGbqLoader, GcsLoader
from src.etl.extractors import FakeDataExtractor
from src.etl.transformers import PandasTransformer
from src.etl import execute_tasks
from pathlib import Path
import yaml

if __name__ == '__main__':

    # write your code here
    execute_tasks(Path('test_tasks.yml'))
