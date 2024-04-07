# script to preprocess single row of data to input into model
import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
import joblib
from datetime import datetime
from typing import Tuple
import requests
import json

# GLOBALS
DATA_PATH = '../airflow/dags/data/resale_flats_transformed.csv'
CPI_PATH = '../airflow/dags/data/cpi_with_lag_sma_ema.csv'
SIBOR_PATH = '../airflow/dags/data/sibor_sora.csv'
FEATURE_SCALER_PATH = '_scalers/feature_scaler.save'
TARGET_SCALER_PATH = '_scalers/target_scaler.save'
NUM_COLS = ['floor_area_sqm', 'remaining_lease', 'year', 'cpi', 'cpi_lag1',
            'cpi_lag3', 'cpi_lag6', 'cpi_lag12', 'cpi_sma3', 'cpi_sma6',
            'cpi_sma12', 'cpi_ema3', 'cpi_ema6', 'cpi_ema12', 'SIBOR 1M',
            'SIBOR 3M', 'SIBOR 6M', 'SIBOR 12M', 'SOR ON', 'SOR 1M',
            'SOR 3M', 'SOR 6M']


def load_external_data(cpi_file: str, sibor_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load CPI and SIBOR data from files."""
    cpi_data = pd.read_csv(cpi_file)
    cpi_data['Month'] = pd.to_datetime(cpi_data['Month'])
    cpi_data.rename(columns={'Value': 'cpi'}, inplace=True)
    sibor_data = pd.read_csv(sibor_file)
    sibor_data['SIBOR DATE'] = pd.to_datetime(sibor_data['SIBOR DATE'])
    return cpi_data, sibor_data


def find_relevant_external_data(year: int, month: int, day: int, cpi_data: pd.DataFrame, sibor_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Find relevant CPI and SIBOR data for the given year, month, and day."""
    # Adjusting to use datetime for comparison, considering day as well
    target_date = pd.Timestamp(year=year, month=month, day=day)
    cpi_row = cpi_data[cpi_data['Month'] == target_date]
    sibor_row = sibor_data[sibor_data['SIBOR DATE'] == target_date]
    cpi_row.drop('Month', axis=1, inplace=True)
    sibor_row.drop('SIBOR DATE', axis=1, inplace=True)
    return cpi_row, sibor_row


def convert_remaining_lease(years: int, months: int) -> int:
    """Convert remaining lease to total months."""
    return years * 12 + months


def preprocess_input(input_data: pd.DataFrame, scaler: StandardScaler, cpi_row: pd.DataFrame, sibor_row: pd.DataFrame) -> pd.DataFrame:
    """Preprocess a single row of input data."""
    # Merge CPI and SIBOR data with input_data
    for column in cpi_row.columns:
        input_data[column] = cpi_row[column].values[0]
    for column in sibor_row.columns:
        input_data[column] = sibor_row[column].values[0]

    # Assuming 'year' and 'month' columns are present for sale_date
    input_data['sale_date'] = datetime(
        input_data['year'], input_data['month'], 1)

    # TODO: sus
    # Load the dummies from the training set
    train_set = pd.read_csv(DATA_PATH)
    categorical_vars = ['flat_type', 'storey_range',
                        'flat_model', 'district', 'month']
    train_set_dummies = pd.get_dummies(train_set, columns=categorical_vars)
    # Apply the dummies to the input data
    input_data_dummies = pd.get_dummies(input_data, columns=categorical_vars)
    # Ensure input data has all dummy variables present in the training set
    for column in train_set_dummies.columns:
        if column not in input_data_dummies.columns:
            # Add missing dummy variables as columns with 0s
            input_data_dummies[column] = 0

    # Select and scale numerical columns
    num_cols = NUM_COLS

    input_data = input_data_dummies.copy()
    input_data[num_cols] = scaler.transform(input_data_dummies[num_cols])

    # Drop 'sale_date' if not used as a feature
    input_data.drop('sale_date', axis=1, inplace=True)

    print(f"INPUT DATA COLUMNS {input_data.columns}")
    return input_data


def predict(preprocessed_input, port):
    """Send preprocessed input data to the MLflow model serving endpoint for prediction."""
    # Convert the DataFrame to a JSON format that the model serving endpoint expects
    # Adjusting to use 'dataframe_split' format
    data = preprocessed_input.to_dict(orient='split')
    del data['index']  # Remove 'index' if present
    # Adjusting the data format to match the expected 'dataframe_split' format
    formatted_data = {'dataframe_split': data}
    # Prepare headers
    headers = {'Content-Type': 'application/json'}
    # URL of the model serving endpoint
    url = f"http://localhost:{port}/invocations"
    # Make the POST request and get the response
    response = requests.post(url, headers=headers,
                             data=json.dumps(formatted_data))
    # Check for successful response
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Prediction request failed with status code {response.status_code} and message: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Preprocess input data for model prediction.')
    parser.add_argument('--port', type=int, required=True,
                        help='Port number of the model serving endpoint')
    parser.add_argument('--flat_type', type=str,
                        required=True, help='Type of the flat')
    parser.add_argument('--storey_range', type=str,
                        required=True, help='Storey range of the flat')
    parser.add_argument('--flat_model', type=str,
                        required=True, help='Model of the flat')
    parser.add_argument('--district', type=str,
                        required=True, help='District of the flat')
    parser.add_argument('--month', type=int, required=True, help='Sale month')
    parser.add_argument('--year', type=int, required=True, help='Sale year')
    parser.add_argument('--remaining_lease_years', type=int,
                        required=True, help='Remaining lease in years')
    parser.add_argument('--remaining_lease_months', type=int,
                        default=0, help='Remaining lease in months (optional)')
    parser.add_argument('--floor_area_sqm', type=float,
                        required=True, help='Floor area in square meters')
    # Add more arguments as necessary for your model's input features

    args = parser.parse_args()

    # Load external data
    cpi_data, sibor_data = load_external_data(CPI_PATH, SIBOR_PATH)
    cpi_row, sibor_row = find_relevant_external_data(
        args.year, args.month, 1, cpi_data, sibor_data)

    # Create a DataFrame from the parsed arguments
    input_row = pd.DataFrame([vars(args)])

    # Convert remaining lease to total months
    input_row['remaining_lease'] = convert_remaining_lease(
        args.remaining_lease_years, args.remaining_lease_months)

    # import scaler
    scaler: StandardScaler = joblib.load(FEATURE_SCALER_PATH)

    # Preprocess the input row
    preprocessed_input = preprocess_input(
        input_row, scaler, cpi_row, sibor_row)

    # Get the prediction
    prediction = predict(preprocessed_input, args.port)
    prediction = prediction['predictions']  # this is a list with 1 element
    # list to numpy array and reshape (-1, 1)
    prediction = np.array(prediction).reshape(-1, 1)

    # inverse transform the prediction
    target_scaler: StandardScaler = joblib.load(TARGET_SCALER_PATH)
    prediction = target_scaler.inverse_transform(prediction)  # throws error

    print(f"Predicted price: ${prediction[0][0]:,.2f}")
    # features
    # output
    # time at which prediction was made
