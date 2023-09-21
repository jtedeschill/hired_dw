import unittest
from unittest.mock import patch, mock_open
from pathlib import Path
from src.etl import execute_tasks

class TestExecuteTasks(unittest.TestCase):

    @patch("your_module.yaml.safe_load")  # Replace 'your_module' with the actual module where 'yaml' is imported
    @patch("your_module.logging.info")  # Replace 'your_module' with the actual module where 'logging' is imported
    @patch("your_module.load_registry")  # Replace 'your_module' with the actual module where 'load_registry' is defined
    def test_execute_tasks(self, mock_load_registry, mock_logging_info, mock_yaml_load):

        # Mocking the return value for 'load_registry'
        mock_load_registry.return_value = {
            "TaskType": MockTask()  # MockTask should be a mock of your actual Task class
        }

        # Mocking the yaml content loaded from the file
        mock_yaml_content = [
            {
                "name": "task1",
                "type": "TaskType",
                "extractor": {
                    "class": "ExtractorClass",
                },
                "loader": {
                    "class": "LoaderClass",
                }
            }
        ]
        mock_yaml_load.return_value = mock_yaml_content

        # Mocking open function to simulate reading from a file
        m = mock_open()
        with patch("builtins.open", m):
            execute_tasks(Path("test_tasks.yml"))

        # Assertions to make sure logging.info was called with expected arguments
        mock_logging_info.assert_any_call("Loaded 1 tasks")
        mock_logging_info.assert_any_call("Running task task1")
        mock_logging_info.assert_any_call("Finished task task1")

        # Add more assertions as necessary to verify behavior

# Mock class to simulate your actual Task class
class MockTask:
    def initialize(self, name, extractor, loader, params):
        pass

    def run(self):
        pass

if __name__ == "__main__":
    unittest.main()
