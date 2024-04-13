# simple streamlit app to demonstrate the model
from predictor import Predictor
import streamlit as st
import numpy as np
import pandas as pd

# Initialize the Predictor
predictor = Predictor(8002)  # Assuming the ML server is running on port 8002

# Load district data
districts_df = pd.read_excel('../../data/districts.xlsx')
# Assuming 'General Location' is a column in the Excel file that maps to each district
general_locations = districts_df['General Location'].unique()

st.title("HDB Resale Price Predictor")

# Input fields to match the structure of 'example.csv'
year = st.number_input('Year', min_value=2000, max_value=2100, value=2023)
month = st.number_input('Month', min_value=1, max_value=12, value=4)
floor_area_sqm = st.number_input('Floor Area (sqm)', min_value=1, value=68)
remaining_lease_years = st.number_input(
    'Remaining Lease (Years)', min_value=0, value=95)
remaining_lease_months = st.number_input(
    'Remaining Lease (Months)', min_value=0, max_value=11, value=0)
flat_type = st.selectbox("Flat Type", [
                         '1 ROOM', '2 ROOM', '3 ROOM', '4 ROOM', '5 ROOM', 'EXECUTIVE', 'MULTI-GENERATION'])
storey_range = st.selectbox("Storey Range", ['01 TO 03', '04 TO 06', '07 TO 09', '10 TO 12', '13 TO 15', '16 TO 18', '19 TO 21',
                            '22 TO 24', '25 TO 27', '28 TO 30', '31 TO 33', '34 TO 36', '37 TO 39', '40 TO 42', '43 TO 45', '46 TO 48', '49 TO 51'])
flat_model = st.selectbox("Flat Model", ['Model A', 'Model A2', 'Model B', 'Model B1', 'Model B2', 'Model C', 'Model C2', 'Model D',
                          'Model E', 'Model F', 'Improved', 'New Generation', 'DBSS', 'Standard', 'Apartment', 'Maisonette', 'Terrace', 'Type S1', 'Type S2'])
general_location = st.selectbox("General Location", general_locations)

# Button to submit and predict
if st.button("Submit"):

    # Map the selected general location back to its district code(s)
    selected_districts = districts_df[districts_df['General Location'] == general_location]['Postal District'].tolist()
    # Assuming we want to use the first district code for simplicity or a specific logic is applied to select the district code
    selected_district_code = selected_districts[0] if selected_districts else None

    # Create a DataFrame with the input data
    input_df = pd.DataFrame([{
        'year': year,
        'month': month,
        'floor_area_sqm': floor_area_sqm,
        'remaining_lease_years': remaining_lease_years,
        'remaining_lease_months': remaining_lease_months,
        'flat_type': flat_type,
        'storey_range': storey_range,
        'flat_model': flat_model,
        'district': selected_district_code
    }])

    # Call the predict_df method
    predictions = predictor.predict_df(input_df)

    # Display the predictions
    st.write('The predicted transaction price is: $', round(predictions['predicted_price'][0], 2))
