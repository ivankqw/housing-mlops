import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
import joblib
from datetime import datetime
from typing import Tuple
import requests
import json
import warnings
warnings.filterwarnings('ignore')

# GLOBALS
DATA_PATH = '../../data/resale_flats_transformed.csv'
CPI_PATH = '../../data/cpi_with_lag_sma_ema.csv'
SIBOR_PATH = '../../data/sibor_sora.csv'
FEATURE_SCALER_PATH = '_scalers/feature_scaler.save'
TARGET_SCALER_PATH = '_scalers/target_scaler.save'
NUM_COLS = ['floor_area_sqm', 'remaining_lease', 'year', 'cpi', 'cpi_lag1',
            'cpi_lag3', 'cpi_lag6', 'cpi_lag12', 'cpi_sma3', 'cpi_sma6',
            'cpi_sma12', 'cpi_ema3', 'cpi_ema6', 'cpi_ema12', 'SIBOR 1M',
            'SIBOR 3M', 'SIBOR 6M', 'SIBOR 12M', 'SOR ON', 'SOR 1M',
            'SOR 3M', 'SOR 6M']


class Predictor:
    def __init__(self, port: int):
        self.port = port
        self.cpi_data, self.sibor_data = self.load_external_data(
            CPI_PATH, SIBOR_PATH)
        self.feature_scaler: StandardScaler = joblib.load(FEATURE_SCALER_PATH)
        self.target_scaler: StandardScaler = joblib.load(TARGET_SCALER_PATH)

    @staticmethod
    def load_external_data(cpi_file: str, sibor_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load CPI and SIBOR data from files."""
        cpi_data = pd.read_csv(cpi_file)
        cpi_data['Month'] = pd.to_datetime(cpi_data['Month'])
        cpi_data.rename(columns={'Value': 'cpi'}, inplace=True)
        sibor_data = pd.read_csv(sibor_file)
        sibor_data['SIBOR DATE'] = pd.to_datetime(sibor_data['SIBOR DATE'])
        return cpi_data, sibor_data

    @staticmethod
    def find_relevant_external_data(year: int, month: int, day: int, cpi_data: pd.DataFrame, sibor_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Find relevant CPI and SIBOR data for the given year, month, and day."""
        target_date = pd.Timestamp(year=year, month=month, day=day)
        cpi_row = cpi_data[cpi_data['Month'] == target_date]
        sibor_row = sibor_data[sibor_data['SIBOR DATE'] == target_date]
        cpi_row.drop('Month', axis=1, inplace=True)
        sibor_row.drop('SIBOR DATE', axis=1, inplace=True)
        return cpi_row, sibor_row

    @staticmethod
    def get_latest_relevant_external_data(cpi_data: pd.DataFrame, sibor_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get the latest relevant CPI and SIBOR data."""
        cpi_row = cpi_data.iloc[-1]
        sibor_row = sibor_data.iloc[-1]
        # convert to dataframe
        cpi_row = pd.DataFrame(cpi_row).T
        sibor_row = pd.DataFrame(sibor_row).T
        return cpi_row, sibor_row

    @staticmethod
    def convert_remaining_lease(years: int, months: int) -> int:
        """Convert remaining lease to total months."""
        return years * 12 + months

    import pandas as pd

    def data_quality_checks(self, input_data: pd.DataFrame) -> None:
        """Perform data quality checks on the input data."""
        print("Performing data quality checks on the input data...")

        def check_column(data, column_name, condition, error_message):
            try:
                if not condition(data[column_name]):
                    raise ValueError(error_message)
            except (TypeError, ValueError) as e:
                print(f"Error in column '{column_name}': {e}")
                raise e

        # Check for invalid values in 'year'
        check_column(input_data, 'year', lambda x: (x < 0).any() or (lambda x : isinstance((int,float))), "Year must be a non-negative integer.")
        #  if (input_data['year'] < 0).any() or ~(input_data['year'].apply(lambda x: isinstance(x, (int,float)))).all():

        # Check for invalid values in 'month'
        check_column(input_data, 'month', lambda x: (x.between(1, 12) & (x.astype(int) == x)).all(), "Month must be an integer between 1 and 12.")

        # Check for invalid values in 'remaining_lease_years'
        check_column(input_data, 'remaining_lease_years', lambda x: (x >= 0).all() and (x.astype(int) == x).all(), "Remaining lease must be a non-negative integer.")

        # Check for invalid values in 'floor_area_sqm'
        check_column(input_data, 'floor_area_sqm', lambda x: (x >= 0).all(), "Floor area must be a non-negative float or integer.")

        # Check for invalid values in 'flat_type'
        valid_flat_types = ['1 ROOM', '2 ROOM', '3 ROOM', '4 ROOM', '5 ROOM', 'EXECUTIVE', 'MULTI-GENERATION']
        check_column(input_data, 'flat_type', lambda x: x.isin(valid_flat_types).all(), "Invalid flat type.")

        # Check for valid data types in 'flat_model', 'storey_range', and 'district'
        check_column(input_data, 'flat_model', lambda x: x.apply(lambda y: isinstance(y, str)).all(), "Flat model must be a string.")
        check_column(input_data, 'storey_range', lambda x: x.apply(lambda y: isinstance(y, str)).all(), "Storey range must be a string.")
        check_column(input_data, 'district', lambda x: x.apply(lambda y: isinstance(y, (int,float))).all(), "District must be a string.")

        print("Data quality checks completed successfully.")

    def preprocess_input(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Preprocess a single row of input data."""
        try:
            self.data_quality_checks(input_data)
            print("Data quality checks passed.")
        except (TypeError,ValueError) as e:
            print(f"Data quality checks failed: {e}, Ignoring row.")
            return None
        try:
            cpi_row, sibor_row = self.find_relevant_external_data(
                int(input_data['year'].iloc[0]), int(input_data['month'].iloc[0]), 1, self.cpi_data, self.sibor_data)
        except (ValueError, OverflowError) as e:
            print(f"Fail to find relevant external data,Error: {e}")
            return None

        for column in cpi_row.columns:
            try:
                input_data[column] = cpi_row[column].values[0]
            except IndexError:
                cpi_row, sibor_row = self.get_latest_relevant_external_data(
                    self.cpi_data, self.sibor_data)
                break

        try:
            # Merge CPI and SIBOR data with input_data
            for column in cpi_row.columns:
                input_data[column] = cpi_row[column].values[0]

            for column in sibor_row.columns:
                input_data[column] = sibor_row[column].values[0]
        except IndexError:
            print("Error: No relevant CPI and SIBOR data found.")
            return None

        # Define the mapping from flat_type to an ordinal number
        flat_type_mapping = {
            '1 ROOM': 1,
            '2 ROOM': 2,
            '3 ROOM': 3,
            '4 ROOM': 4,
            '5 ROOM': 5,
            'EXECUTIVE': 6,
            'MULTI-GENERATION': 7,
        }

        # Apply the mapping to the 'flat_type' column
        input_data['flat_type_ordinal'] = input_data['flat_type'].replace(flat_type_mapping)

        # Optionally, if you no longer need the original 'flat_type' column, you can drop it
        input_data.drop('flat_type', axis=1, inplace=True)

        # Load the dummies from the training set
        train_set = pd.read_csv(DATA_PATH)
        categorical_vars = ['storey_range', 'flat_model', 'district', 'month']
        train_set_dummies = pd.get_dummies(train_set, columns=categorical_vars)
        input_data_dummies = pd.get_dummies(
            input_data, columns=categorical_vars)

        # Ensure input data has all dummy variables present in the training set
        for column in train_set_dummies.columns:
            if column not in input_data_dummies.columns:
                input_data_dummies[column] = 0

        input_data_preprocessed = input_data_dummies[NUM_COLS + ['flat_type_ordinal'] + [
            col for col in train_set_dummies.columns if col not in NUM_COLS]]
        try:
            # Scale the numerical features
            input_data_preprocessed[NUM_COLS] = self.feature_scaler.transform(
                input_data_preprocessed[NUM_COLS])
        except ValueError as e:
            print(f"Failed to sclae numerical features, Error: {e}")
            return None

        return input_data_preprocessed

    def predict(self, **kwargs) -> float:
        """Predict the resale price for a given set of features."""
        input_data = pd.DataFrame([kwargs])
        input_data_preprocessed = self.preprocess_input(input_data)

        if input_data_preprocessed is None:
            return None
        print(input_data_preprocessed)

        # Adjusting to use 'dataframe_split' format for prediction
        data = input_data_preprocessed.to_dict(orient='split')
        del data['index']  # Remove 'index' if present
        formatted_data = {'dataframe_split': data}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            f'http://localhost:{self.port}/invocations', headers=headers, data=json.dumps(formatted_data))

        if response.status_code == 200:
            prediction = response.json()
        else:
            raise Exception(
                f"Prediction request failed with status code {response.status_code} and message: {response.text}")

        # Inverse transform the predicted price
        predicted_price = self.target_scaler.inverse_transform(
            np.array(prediction['predictions']).reshape(-1, 1))[0][0]

        print(f"Predicted price: ${predicted_price:.2f}")
        return predicted_price

    def predict_csv(self, csv_path: str) -> pd.DataFrame:
        """Predict resale prices for a CSV file of input features."""
        input_data = pd.read_csv(csv_path)
        predictions = []

        for _, row in input_data.iterrows():
            row_dict = row.to_dict()
            prediction = self.predict(**row_dict)
            predictions.append(prediction)

        input_data['predicted_price'] = predictions
        return input_data

    def predict_df(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Predict resale prices for a DataFrame of input features."""
        predictions = []

        for _, row in input_data.iterrows():
            row_dict = row.to_dict()
            prediction = self.predict(**row_dict)
            predictions.append(prediction)

        input_data['predicted_price'] = predictions
        return input_data

# if __name__ == "__main__":

    # test_files = ["tests/test_extreme_values.csv", "tests/test_null_values.csv"]
    # test_files = ["tests/test_data_type.csv",
    #               "tests/test_extreme_values.csv", "tests/test_null_values.csv"]
    # test_files = ["tests/test_data_type.csv"]
    # predictor = Predictor(8002)
    # for test_file in test_files:
    #     print(test_file)
    #     try:
    #         predictor.predict_csv(test_file)
    #     except Exception as e:
    #         print(f"Failed to predict for {test_file}: {e}")
    #         raise e
    # print("All rows processed.")

