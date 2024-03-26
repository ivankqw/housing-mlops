from typing import Dict
from airflow.decorators import task
import requests
import pandas as pd
from cfg import DATA_PATH, ADDRESSES_CSV, ONEMAP_API_URL


def read_districts(data_path: str = DATA_PATH):
    return pd.read_excel(data_path + "districts.xlsx")

def transform_districts(districts: pd.DataFrame):
    districts["Postal Sector"] = districts["Postal Sector"].str.split(",").explode().str.strip()
    districts.drop(columns=["General Location"], inplace=True)
    districts.drop_duplicates(inplace=True)
    districts = districts[districts["Postal Sector"].str.len() == 2]
    return districts

def get_info_from_street_name(address: str):
    cache = load_address_cache()
    if address in cache:
        return cache[address]
    response = requests.get(f"{ONEMAP_API_URL}?searchVal={address}&returnGeom=Y&getAddrDetails=Y")
    result = response.json()["results"][0]
    info = (result["POSTAL"], result["X"], result["Y"], result["LATITUDE"], result["LONGITUDE"])
    cache[address] = info
    save_address_cache(cache)
    return info

# function to get district from postal code from districts table
def get_district_from_postal(postal: str, districts: pd.DataFrame):
    postal_sector = str(postal)[:2]
    district = districts.get(postal_sector, "NIL")
    return district


def transform_resale_flats(resale_flats: str, districts: pd.DataFrame):
    resale_flats["year"], resale_flats["month"] = resale_flats["month"].str.split("-").str
    resale_flats = resale_flats[resale_flats["year"].astype(int) >= 2017]
    resale_flats.sort_values(by=["year", "month"], ascending=False, inplace=True)
    resale_flats["street_name_with_block"] = resale_flats["block"] + " " + resale_flats["street_name"]
    resale_flats["postal"], resale_flats["x"], resale_flats["y"], resale_flats["lat"], resale_flats["lon"] = \
        zip(*resale_flats["street_name_with_block"].apply(lambda x: get_info_from_street_name(x)))
    resale_flats["district"] = resale_flats["postal"].apply(lambda x: get_district_from_postal(x, districts))
    resale_flats = resale_flats[resale_flats["district"] != "NIL"]
    return resale_flats

# def transform_rental_flats(filename: str, districts: pd.DataFrame):
#     rental_flats = pd.read_csv(DATA_PATH + filename)
#     # Split 'rent_approval_date' into 'year' and 'month' columns
#     rental_flats[['year', 'month']] = rental_flats['rent_approval_date'].str.split('-', expand=True)
#     rental_flats.drop(columns=['rent_approval_date'], inplace=True)
#     rental_flats["street_name_with_block"] = rental_flats["block"] + " " + rental_flats["street_name"]
#     rental_flats[['postal', 'x', 'y', 'lat', 'lon']] = rental_flats['street_name_with_block'].apply(
#         lambda address: get_info_from_street_name(address)
#         ).tolist()
#     # Use the existing get_district_from_postal function
#     rental_flats["district"] = rental_flats["postal"].apply(lambda postal: get_district_from_postal(postal, districts))
#     # Filter out rows with 'NIL' district
#     rental_flats = rental_flats[rental_flats["district"] != "NIL"]

#     return rental_flats

def load_address_cache():
    try:
        addresses = pd.read_csv(DATA_PATH + ADDRESSES_CSV)
        return {row["address"]: (row["postal"], row["x"], row["y"], row["lat"], row["lon"]) for index, row in addresses.iterrows()}
    except FileNotFoundError:
        return {}

def save_address_cache(cache):
    df = pd.DataFrame.from_dict(cache, orient="index", columns=["postal", "x", "y", "lat", "lon"])
    df.index.name = "address"
    df.to_csv(DATA_PATH + ADDRESSES_CSV)


@task
def transform_districts_task():
    districts_df = read_districts()
    districts_df_transformed = transform_districts(districts_df)
    districts_df_transformed.to_csv(DATA_PATH + "districts_transformed.csv", index=False)
    return DATA_PATH + "districts_transformed.csv"

@task()
def transform_resale_flat_transactions_task(datagov_dict: Dict[str, str], districts_transformed_path: str):
    districts_df_transformed = pd.read_csv(districts_transformed_path)
    resale_flats_df = pd.read_csv(DATA_PATH + datagov_dict["df_resale_flat_transactions"])
    resale_flats_df_transformed = transform_resale_flats(resale_flats_df, districts_df_transformed)
    data_path_resale_flats = DATA_PATH + "resale_flats_transformed.csv"
    resale_flats_df_transformed.to_csv(data_path_resale_flats, index=False)
    return data_path_resale_flats

# @task
# def transform_rental_flats_task(datagov_dict: Dict[str, str], districts_transformed_path: str):
#     df_flat_rental_filename = datagov_dict["df_flat_rental"]
#     districts_df_transformed = pd.read_csv(districts_transformed_path)

#     print("Transforming rental flats...")
#     df_rental_flats = transform_rental_flats(  
#         df_flat_rental_filename, districts_df_transformed
#     )

#     data_path = "/opt/airflow/dags/data"
#     data_path_rental_flats = data_path + "/rental_flats_transformed.csv"
#     df_rental_flats.to_csv(data_path_rental_flats, index=False)
#     return data_path_rental_flats