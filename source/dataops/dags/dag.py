from airflow.decorators import dag
from datetime import datetime, timedelta
from extract_ura import extract_ura_data_task
from extract_singstat import extract_cpi_task
from extract_datagovsg import extract_datagovsg_data_task
from extract_singstat import extract_cpi_task
from extract_datagovsg import extract_datagovsg_data_task
from extract_ura import extract_ura_data_task
from transform_datagovsg import (
    transform_districts_task,
    transform_resale_flat_transactions_task,
)
from transform_ura import transform_private_transactions_task
from load import load_singstat, load_ura, load_datagovsg
from email_dataprofiling import send_data_profiling_email

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bt4301_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["twk"],
)
def property_pipeline():

    # Extract
    data_path_cpi = extract_cpi_task()
    data_path_private_transactions = extract_ura_data_task()
    datagov_dict = extract_datagovsg_data_task()

    # send data quality email after ingestion
    send_data_profiling_email(data_path_cpi)
    send_data_profiling_email(data_path_private_transactions)
    send_data_profiling_email(datagov_dict["df_resale_flat_transactions"])

    # Transform
    data_path_private_transactions = transform_private_transactions_task(
        data_path_private_transactions
    )
    districts_transformed_path = transform_districts_task()
    data_path_resale_flats = transform_resale_flat_transactions_task(
        datagov_dict, districts_transformed_path
    )

    # Load
    load_singstat(data_path_cpi)
    load_ura(data_path_private_transactions)
    load_datagovsg(data_path_resale_flats, datagov_dict["df_hdb_information"])

    # send data profiling email after loading
    send_data_profiling_email(data_path_cpi)
    send_data_profiling_email(data_path_private_transactions)
    send_data_profiling_email(data_path_resale_flats)
    send_data_profiling_email(datagov_dict["df_hdb_information"])


property_pipeline()
