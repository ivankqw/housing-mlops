import json
import http.client
import requests
import pandas as pd
from airflow.decorators import task
from cfg import DATA_PATH, URA_ACCESS_KEY

def get_token(access_key): 
    conn = http.client.HTTPSConnection("www.ura.gov.sg")
    payload = ''
    headers = {
    'AccessKey': access_key
    }
    conn.request("GET", "/uraDataService/insertNewToken.action", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))['Result']

# def get_result(token, access_key, route):
#     conn = http.client.HTTPSConnection("www.ura.gov.sg")
#     payload = ""
#     headers = {
#         "AccessKey": access_key,
#         "Token": token,
#         "User-Agent": "PostmanRuntime/7.26.8",
#     }
#     conn.request("GET", route, payload, headers)
#     res = conn.getresponse()
#     data = res.read()
#     data = json.loads(data.decode("utf-8"))['Result']
#     return data

def get_result(token, access_key, route):
    headers = {
        "AccessKey": access_key,
        "Token": token,
        "User-Agent": "PostmanRuntime/7.26.8",
    }
    res = requests.get("https://www.ura.gov.sg" + route, headers=headers)
    data = res.json()['Result']
    return data

def get_all_results(token, access_key, routes):
    result = []
    for route in routes:
        result.extend(get_result(token, access_key, route))
    return result

def get_all_ura():
    private_transactions_routes = [
        "/uraDataService/invokeUraDS?service=PMI_Resi_Transaction&batch=" + str(i)
        for i in range(1, 5)
    ]

    access_key = URA_ACCESS_KEY
    token = get_token(access_key)

    df_private_transactions = pd.json_normalize(
        data = get_all_results(token, access_key, private_transactions_routes), 
        record_path='transaction', 
        meta = ['street', 'project', 'marketSegment']
        )

    return df_private_transactions


@task
def extract_ura_data_task():
    print("Getting URA data...")
    df_private_transactions = get_all_ura()
    print("URA data obtained.")

    data_path_private_transactions = DATA_PATH + "/private_transactions.csv"

    # save to csv
    print("Saving to csv...")
    df_private_transactions.to_csv(data_path_private_transactions, index=False)
    print("done")
    return data_path_private_transactions