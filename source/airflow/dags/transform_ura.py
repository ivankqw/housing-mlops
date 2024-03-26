from airflow.decorators import task
import pandas as pd
from cfg import DATA_PATH

def private_get_month_year(date):
    date = str(date)
    # date is in format of MYY or MMYY
    # if date is in format of MYY, add 0 in front
    if len(date) == 3:
        date = "0" + date
    month = date[:2]
    year = date[2:]
    # add prefix to year 20
    year = "20" + year
    return (month, year)

@task()
def transform_private_transactions_task(file_path_private_transactions : str):
    print("Transforming private transactions")
    private_transactions = pd.read_csv(file_path_private_transactions)
    # add a column _id
    private_transactions.insert(0, "_id", range(1, 1 + len(private_transactions)))
    # apply private_get_month_year function to each row, creating 2 new columns for month and year
    private_transactions["month"], private_transactions["year"] = zip(
        *private_transactions["contractDate"].apply(private_get_month_year)
    )
    data_path_private_transactions = (
        DATA_PATH + "/private_transactions_transformed.csv"
    )
    private_transactions.to_csv(data_path_private_transactions, index=False)
    
    return data_path_private_transactions