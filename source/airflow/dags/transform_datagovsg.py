from typing import Dict
from airflow.decorators import task
import requests
import pandas as pd
from cfg import DATA_PATH, ADDRESSES_CSV, ONEMAP_API_URL


def read_districts():
    return pd.read_excel(DATA_PATH + "districts.xlsx")

def transform_districts(districts: pd.DataFrame):
    districts["Postal Sector"] = districts["Postal Sector"].str.split(",").explode().reset_index(drop=True).str.strip()
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


def transform_resale_flats(resale_flats: pd.DataFrame, districts: pd.DataFrame):
    resale_flats["month"] = resale_flats["month"].str.split("-")
    resale_flats["year"] = resale_flats["month"].apply(lambda x: int(x[0]))
    resale_flats["month"] = resale_flats["month"].apply(lambda x: int(x[1]))
    resale_flats = resale_flats[resale_flats["year"].astype(int) >= 2017]
    resale_flats.sort_values(by=["year", "month"], ascending=False, inplace=True)
    resale_flats["street_name_with_block"] = resale_flats["block"] + " " + resale_flats["street_name"]
    resale_flats["postal"], resale_flats["x"], resale_flats["y"], resale_flats["lat"], resale_flats["lon"] = \
        zip(*resale_flats["street_name_with_block"].apply(lambda x: get_info_from_street_name(x)))
    resale_flats["district"] = resale_flats["postal"].apply(lambda x: get_district_from_postal(x, districts))
    resale_flats = resale_flats[resale_flats["district"] != "NIL"]
    return resale_flats

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
    resale_flats_df = pd.read_csv(datagov_dict["df_resale_flat_transactions"])
    resale_flats_df_transformed = transform_resale_flats(resale_flats_df, districts_df_transformed)
    data_path_resale_flats = DATA_PATH + "resale_flats_transformed.csv"
    resale_flats_df_transformed.to_csv(data_path_resale_flats, index=False)
    return data_path_resale_flats
