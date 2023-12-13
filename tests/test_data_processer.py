import unittest
from unittest.mock import MagicMock
import pandas as pd
import sys
sys.path.append('/home/ekram/Desktop/week-1/user_analytics_in_the_telecommunication_industry')
from src.data_processer import TelecomDataProcessor

class TestTelecomDataProcessor(unittest.TestCase):

    def setUp(self):
        # Create a sample connection parameter for testing
        self.conn_params = {
            'database': 'tellco',
            'user': 'ekru',
            'password': 'ekram12345',
            'host': 'localhost',
            'port': '5432'
        }

        # Create a sample DataFrame for testing
        data = {
            'Dur. (ms)': [100, 200, 0, 400],
            'Avg RTT DL (ms)': [10, 0, 30, 40],
            # Add other columns as needed
        }

        self.sample_df = pd.DataFrame(data)


    def test_standardize_column_names(self):
        # Create an instance of TelecomDataProcessor
        processor = TelecomDataProcessor(self.conn_params)

        # Apply the standardize_column_names method
        result_df = processor.standardize_column_names(self.sample_df)

        # Assert that the column names are standardized as expected
        expected_data = {
            'DUR._(MS)': [100, 200, 0, 400],
            'AVG_RTT_DL_(MS)': [10, 0, 30, 40],
            # Adjust the expected values based on your sample data
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_handle_missing_values(self):
        # Create an instance of TelecomDataProcessor
        processor = TelecomDataProcessor(self.conn_params)

        # Apply the handle_missing_values method
        result_df = processor.handle_missing_values(self.sample_df)

        # Print actual, expected values for debugging
        print("Original values:")
        print(self.sample_df.to_dict(orient="list"))
        print("After handling missing values:")
        print(result_df.to_dict(orient="list"))
        print("Expected values:")

        # Assert that missing values are filled with the mean as expected
        expected_data = {
            'Dur. (ms)': [100, 200, 233.33333333333334, 400],
            'Avg RTT DL (ms)': [10, 26.666666666666668, 30, 40],
            # Adjust the expected values based on your sample data
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result_df, expected_df)

if __name__ == '__main__':
    unittest.main()
