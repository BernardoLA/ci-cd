import unittest
import pandas as pd
from pipeline import DataPipeline  # Import the production code

class TestDataPipeline(unittest.TestCase):
    def setUp(self):
        """Set up a sample DataFrame for testing."""
        self.pipeline = DataPipeline()
        self.df = pd.DataFrame({
            'name': ['Product1', 'Product2', None],
            'price': [10, 20, 30],
            'quantity': [2, 3, 4]
        })

    def test_clean_data(self):
        """Test if clean_data removes rows with null 'name'."""
        result = self.pipeline.clean_data(self.df)
        self.assertEqual(len(result), 2)  # Two rows should remain

    def test_transform_data(self):
        """Test if transform_data correctly calculates revenue."""
        result = self.pipeline.transform_data(self.df)
        expected_revenue = [40, 60, 120]
        self.assertListEqual(result['revenue'].tolist(), expected_revenue)

    def test_full_pipeline(self):
        """Test the full pipeline process."""
        # Simulate running the pipeline without saving to file
        df_cleaned = self.pipeline.clean_data(self.df)
        df_transformed = self.pipeline.transform_data(df_cleaned)
        
        # Check that the final DataFrame is as expected
        self.assertEqual(len(df_transformed), 2)  # Only two rows should remain
        self.assertIn('revenue', df_transformed.columns)  # 'revenue' column exists

if __name__ == '__main__':
    unittest.main()
