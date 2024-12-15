import pandas as pd

class DataPipeline:
    def load_data(self, file_path):
        """Loads data from a CSV file."""
        return pd.read_csv(file_path)

    def clean_data(self, df):
        """Cleans data by removing rows with null values in 'name'."""
        return df.dropna(subset=['name'])

    def transform_data(self, df):
        """Transforms data by adding a 'revenue' column as price * quantity."""
        df['revenue'] = df['price'] * df['quantity']
        return df

    def save_data(self, df, output_path):
        """Saves the DataFrame to a new CSV file."""
        df.to_csv(output_path, index=False)

    def run_pipeline(self, input_path, output_path):
        """Runs the full data pipeline from loading to saving."""
        df = self.load_data(input_path)
        df = self.clean_data(df)
        df = self.transform_data(df)
        self.save_data(df, output_path)
