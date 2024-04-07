import requests
import pandas as pd 
from airflow.decorators import task
from cfg import DATA_PATH
# example call to get the first 100 rows of the dataset
# url = 'https://data.gov.sg/api/action/datastore_search?resource_id=f1765b54-a209-4718-8d38-a39237f502b3&limit=100'


def get_urls(resource_ids, base_url, route):
    return [
        base_url + route + "?resource_id=" + resource_id + "&limit=1000"
        for resource_id in resource_ids.values()
    ]


def get_result(url, base_url) -> list:
    result = []
    while url:
        try:
            curr_json = requests.get(url).json()
            if not result:
                # first time
                result = curr_json["result"]["records"]
            elif curr_json["result"]["records"]:
                result.extend(curr_json["result"]["records"])
            else:  # no more records
                return result
            # check if there is a next page or prev and next not the same
            if ("next" in curr_json["result"]["_links"]) or (
                "prev" in curr_json["result"]["_links"]
                and curr_json["result"]["_links"]["next"]
                != curr_json["result"]["_links"]["prev"]
            ):
                url = base_url + curr_json["result"]["_links"]["next"]
            else:
                url = None
        except Exception as e:
            print(e)
            return result  # return what we have so far
    return result

def get_all_results(start_urls, base_url) -> list:
    result = []
    for url in start_urls:
        result.extend(get_result(url, base_url))
    return result


def run(resource_ids, base_url, route):
    result = get_all_results(get_urls(resource_ids, base_url, route), base_url)
    return pd.DataFrame(result)

def get_resale_flat_transactions():
    base_url = "https://data.gov.sg"
    route = "/api/action/datastore_search"
    resource_ids = {
        "2017_latest": 'f1765b54-a209-4718-8d38-a39237f502b3',
        # "2015_2016": "1b702208-44bf-4829-b620-4615ee19b57c",
        # "2012_2014": "83b2fc37-ce8c-4df4-968b-370fd818138b",
        # "2000_2012": "8c00bf08-9124-479e-aeca-7cc411d884c4",
        # "1990_1999": "adbbddd3-30e2-445f-a123-29bee150a6fe",
    }

    df_resale_flats = run(resource_ids=resource_ids, base_url=base_url, route=route)
    return df_resale_flats


## Pull HDB Property Information from Data.gov.sg
def get_hdb_property_information():
    base_url = "https://data.gov.sg"
    route = "/api/action/datastore_search"
    resource_ids = {"hdb_information": "482bfa14-2977-4035-9c61-c85f871daf4e"}

    df_hdb_information = run(
        resource_ids=resource_ids,
        base_url=base_url,
        route=route,
    )

    return df_hdb_information

@task(multiple_outputs=True)
def extract_datagovsg_data_task():
    print("Getting resale flat transactions...")
    df_resale_flat_transactions = get_resale_flat_transactions()
    print("Getting HDB property information...")
    df_hdb_information = get_hdb_property_information()
    # rename columns in df_hdb_information
    df_hdb_information = df_hdb_information.rename(
        columns = {
            "1room_sold": "one_room_sold",
            "2room_sold": "two_room_sold",
            "3room_sold": "three_room_sold",
            "4room_sold": "four_room_sold",
            "5room_sold": "five_room_sold",
            "1room_rental": "one_room_rental",
            "2room_rental": "two_room_rental",
            "3room_rental": "three_room_rental",
        }
    )

    data_path_resale_flat_transactions = DATA_PATH + "/resale_flat_transactions.csv"
    data_path_hdb_information = DATA_PATH + "/hdb_information.csv"

    # save to csv
    df_resale_flat_transactions.to_csv(
        data_path_resale_flat_transactions, index=False
    )
    df_hdb_information.to_csv(data_path_hdb_information, index=False)

    return {
        "df_resale_flat_transactions": data_path_resale_flat_transactions,
        "df_hdb_information": data_path_hdb_information,
    }