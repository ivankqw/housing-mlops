from typing import Dict
from airflow.decorators import task
import requests
import pandas as pd
from cfg import DATA_PATH, ADDRESSES_CSV, ONEMAP_API_URL


def read_districts():
    return pd.read_excel(DATA_PATH + "districts.xlsx")

import pandas as pd

def transform_districts(districts: pd.DataFrame):
    # Iterate through each row of districts df
    new_data = []

    for index, row in districts.iterrows():
        postal_sector = (row["Postal Sector"])
        # Remove whitespace from entire string and split
        postal_sectors = postal_sector.replace(" ", "").split(",")

        for ps in postal_sectors:
            new_data.append({"Postal District": str(row["Postal District"]), "Postal Sector": str(ps)})

    return pd.DataFrame(new_data, columns=["Postal District", "Postal Sector"])




# function to get district from postal code from districts table
def get_district_from_postal(postal: str, districts: pd.DataFrame):
    postal_sector = str(postal)[:2]
    # print(postal_sector)
    try:
        district = districts[districts["Postal Sector"] == postal_sector]["Postal District"].values[0]
        # print(district)
    except IndexError:
        district = None
        print("district not found: ", postal)
    return district


def transform_resale_flats(resale_flats: pd.DataFrame, districts: pd.DataFrame):
    print("Transforming resale flats data")
    cache = load_address_cache()
    # print len of cache dict
    print(f"Cache length: {len(cache)}")

    def get_info_from_street_name(address: str):
        
        if address in cache:
            return cache[address]
        print(f"Getting info from street name... {address}")
        response = requests.get(f"{ONEMAP_API_URL}?searchVal={address}&returnGeom=Y&getAddrDetails=Y")
        result = response.json()["results"][0]
        info = (result["POSTAL"], result["X"], result["Y"], result["LATITUDE"], result["LONGITUDE"])
        cache[address] = info
        save_address_cache(cache)
        return info
    
    
    print("processing resale flats data...")
    resale_flats["month"] = resale_flats["month"].str.split("-")
    resale_flats["year"] = resale_flats["month"].apply(lambda x: int(x[0]))
    resale_flats["month"] = resale_flats["month"].apply(lambda x: int(x[1]))
    resale_flats = resale_flats[resale_flats["year"].astype(int) >= 2017]
    resale_flats.sort_values(by=["year", "month"], ascending=False, inplace=True)
    resale_flats["street_name_with_block"] = resale_flats["block"] + " " + resale_flats["street_name"]
    print("getting postal code, x, y, lat, lon from street name...")
    resale_flats["postal"], resale_flats["x"], resale_flats["y"], resale_flats["lat"], resale_flats["lon"] = \
        zip(*resale_flats["street_name_with_block"].apply(lambda x: get_info_from_street_name(x)))
    resale_flats["district"] = resale_flats["postal"].apply(lambda x: get_district_from_postal(x, districts))
    # resale_flats = resale_flats[resale_flats["district"] != "NIL"]
    return resale_flats

def load_address_cache():
    print("Loading address cache")
    try:
        addresses = pd.read_csv(DATA_PATH + ADDRESSES_CSV)
        print("address cache found...")
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
    districts_df_transformed = pd.read_csv(districts_transformed_path, dtype=str)
    resale_flats_df = pd.read_csv(datagov_dict["df_resale_flat_transactions"])
    resale_flats_df_transformed = transform_resale_flats(resale_flats_df, districts_df_transformed)
    data_path_resale_flats = DATA_PATH + "resale_flats_transformed.csv"
    resale_flats_df_transformed.to_csv(data_path_resale_flats, index=False)
    return data_path_resale_flats
