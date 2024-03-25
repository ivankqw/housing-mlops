import json
import http.client
import pandas as pd
from airflow.decorators import task

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

def get_result(token, access_key, route):
    conn = http.client.HTTPSConnection("www.ura.gov.sg")
    payload = ""
    headers = {
        "AccessKey": access_key,
        "Token": token,
        "User-Agent": "PostmanRuntime/7.26.8",
    }
    conn.request("GET", route, payload, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data.decode("utf-8"))['Result']
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
    private_rental_routes = [
        f"/uraDataService/invokeUraDS?service=PMI_Resi_Rental&refPeriod={q}"
        # start from 14q1 to 23q1
        for q in [f"{y}q{i}" for y in range(14, 24) for i in range(1, 5)]
    ]  

    planning_decisions_routes = [
        "/uraDataService/invokeUraDS?service=Planning_Decision&year=" + str(i)
        for i in range(2000, 2024)
    ]

    access_key = "480de617-6aee-4c71-a04a-3b6c3a9596b2"
    token = get_token(access_key)

    df_private_transactions = pd.json_normalize(
        data = get_all_results(token, access_key, private_transactions_routes), 
        record_path='transaction', 
        meta = ['street', 'project', 'marketSegment']
        )

    df_private_rental = pd.json_normalize(
        data = get_all_results(token, access_key, private_rental_routes), 
        record_path='rental', 
        meta = ['street', 'project']
        )

    df_planning_decisions = pd.DataFrame(get_all_results(token, access_key, planning_decisions_routes))

    return df_private_transactions, df_private_rental, df_planning_decisions


@task
def extract_ura_data_task():
    print("Getting URA data...")
    (
        df_private_transactions,
        df_private_rental,
        df_planning_decisions,
    ) = get_all_ura()
    print("URA data obtained.")

    data_path = "/opt/airflow/dags/data"
    data_path_private_transactions = data_path + "/private_transactions.csv"
    data_path_private_rental = data_path + "/private_rental.csv"
    data_path_planning_decisions = data_path + "/planning_decisions.csv"

    # save to csv
    print("Saving to csv...")
    df_private_transactions.to_csv(data_path_private_transactions, index=False)
    df_private_rental.to_csv(data_path_private_rental, index=False)
    df_planning_decisions.to_csv(data_path_planning_decisions, index=False)
    print("done")
    return data_path_private_transactions, data_path_private_rental, data_path_planning_decisions