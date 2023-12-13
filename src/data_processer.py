import pandas as pd
import numpy as np
from scipy import stats
import psycopg2

class TelecomDataProcessor:
    def __init__(self, conn_params):
        """
        Initialize the TelecomDataProcessor with the given connection parameters.
        """
        self.conn_params = conn_params
        self.quantitative_columns = [
            'Dur. (ms)',
            'Avg RTT DL (ms)',
            'Avg RTT UL (ms)',
            'Avg Bearer TP DL (kbps)',
            'Avg Bearer TP UL (kbps)',
            'TCP DL Retrans. Vol (Bytes)',
            'TCP UL Retrans. Vol (Bytes)',
            'DL TP < 50 Kbps (%)',
            '50 Kbps < DL TP < 250 Kbps (%)',
            '250 Kbps < DL TP < 1 Mbps (%)',
            'DL TP > 1 Mbps (%)',
            'UL TP < 10 Kbps (%)',
            '10 Kbps < UL TP < 50 Kbps (%)',
            '50 Kbps < UL TP < 300 Kbps (%)',
            'UL TP > 300 Kbps (%)',
            'Activity Duration DL (ms)',
            'Activity Duration UL (ms)',
            'Dur. (ms).1',
            'Nb of sec with 125000B < Vol DL',
            'Nb of sec with 1250B < Vol UL < 6250B',
            'Nb of sec with 31250B < Vol DL < 125000B',
            'Nb of sec with 37500B < Vol UL',
            'Nb of sec with 6250B < Vol DL < 31250B',
            'Nb of sec with 6250B < Vol UL < 37500B',
            'Nb of sec with Vol DL < 6250B',
            'Nb of sec with Vol UL < 1250B',
            'Social Media DL (Bytes)',
            'Social Media UL (Bytes)',
            'Youtube DL (Bytes)',
            'Youtube UL (Bytes)',
            'Netflix DL (Bytes)',
            'Netflix UL (Bytes)',
            'Google DL (Bytes)',
            'Google UL (Bytes)',
            'Email DL (Bytes)',
            'Email UL (Bytes)',
            'Gaming DL (Bytes)',
            'Gaming UL (Bytes)',
            'Other DL (Bytes)',
            'Other UL (Bytes)',
            'Total DL (Bytes)',
            'Total UL (Bytes)'
        ]

    def load_dataset(self):
        """
        Load the dataset from PostgreSQL using the provided connection parameters.
        """
        conn = psycopg2.connect(**self.conn_params)
        sql_query = "SELECT * FROM xdr_data"
        df = pd.read_sql(sql_query, conn)

        conn.close()
        return df

    def remove_outliers(self, df):
        """
        Remove rows with outliers using z-score method.
        """
        z_scores = np.abs(stats.zscore(df[self.quantitative_columns]))
        df_no_outliers = df[(z_scores < 0).all(axis=1)]

        return df_no_outliers

    def handle_missing_values(self, df):
        """
        Handle missing values by filling NaN values with the mean of each column.
        """
        for column in df.columns:
            if df[column].dtype != 'object':  # Exclude object (string) columns
                # Check if the column has NaN or 0 values
                if df[column].isna().any() or (df[column] == 0).any():
                    mean_value = df[column].replace(0, np.nan).mean(skipna=True)
                    print(f"Column: {column}, Mean Value: {mean_value}")
                    df[column] = df[column].replace(0, np.nan).fillna(mean_value)

        return df

    def standardize_column_names(self, df):
        """
        Standardize column names by stripping spaces, converting to lowercase, and replacing spaces with underscores.
        """
        df.columns = df.columns.str.strip().str.upper().str.replace(' ', '_')
        return df

    def clean_dataset(self, df):
        """
        Perform different data cleaning steps on the dataset.
        """
        first_df = self.handle_missing_values(df)
        second_df = self.standardize_column_names(first_df)
        
        return second_df

    def overview_analysis(self, df):
        """
        Display an overview analysis of the dataset.
        """
        print(df.describe())
