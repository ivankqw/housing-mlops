from typing import Dict
from airflow.decorators import task
import requests
import pandas as pd


@task(multiple_outputs=True)
def transform_private_transactions_and_rental(data_path: str, file_path_private_transactions : str, file_path_private_rental : str):
    print("Transforming private transactions and rental...")

    private_transactions = pd.read_csv(file_path_private_transactions)
    private_rental = pd.read_csv(file_path_private_rental)
    
    # add a column _id
    private_transactions.insert(0, "_id", range(1, 1 + len(private_transactions)))
    private_rental.insert(0, "_id", range(1, 1 + len(private_rental)))

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

    # apply private_get_month_year function to each row, creating 2 new columns for month and year
    private_transactions["month"], private_transactions["year"] = zip(
        *private_transactions["contractDate"].apply(private_get_month_year)
    )
    # same for private_rental on leaseDate
    private_rental["month"], private_rental["year"] = zip(
        *private_rental["leaseDate"].apply(private_get_month_year)
    )

    data_path_private_transactions = (
        data_path + "/private_transactions_transformed.csv"
    )
    data_path_private_rental = data_path + "/private_rental_transformed.csv"

    private_transactions.to_csv(data_path_private_transactions, index=False)
    private_rental.to_csv(data_path_private_rental, index=False)

    return {
        "df_private_transactions": data_path_private_transactions,
        "df_private_rental": data_path_private_rental,
    }