from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
# from typing import 

def insert_to_table(hook, table_name, df):
    hook.insert_rows(table=table_name, rows=df.itertuples(index=False))

@task
def load_singstat(data_path_singstat: str):

    create_table_sql = """
        DROP TABLE IF EXISTS cpi;

        CREATE TABLE IF NOT EXISTS cpi (
            Month DATE NOT NULL PRIMARY KEY,
            Value FLOAT
        );
    """

    hook = PostgresHook(postgres_conn_id='bt4301_postgres')
    hook.run(create_table_sql)

    df_cpi = pd.read_csv(data_path_singstat)

    insert_to_table(hook, 'cpi', df_cpi)
    
    return


@task
def load_ura(
    data_path_private_transactions: str,
    data_path_private_rentals: str,
    data_path_planning_decisions: str,
):
    create_table_sql = """
        DROP TABLE IF EXISTS private_transactions;
        DROP TABLE IF EXISTS private_rental;


        CREATE TABLE IF NOT EXISTS private_transactions (
            _id INT NOT NULL PRIMARY KEY,
            area DECIMAL(10,2),
            floor_range VARCHAR(255),
            number_of_units INT,
            contract_date VARCHAR(255),
            type_of_sale INT,
            price INT,
            property_type VARCHAR(255),
            district INT,
            type_of_area VARCHAR(255),
            tenure VARCHAR(255),
            nett_price VARCHAR(255),
            street_name VARCHAR(255),
            project_name VARCHAR(255),
            market_segment VARCHAR(255),
            month INT,
            year INT
        );
        
        CREATE TABLE IF NOT EXISTS private_rental (
            _id INT NOT NULL PRIMARY KEY,
            area_sqm VARCHAR(255),
            lease_date VARCHAR(255),
            property_type VARCHAR(255),
            district INT,
            area_sqft VARCHAR(255),
            number_of_bedrooms DECIMAL(10,2),
            rental INT,
            street_name VARCHAR(255),
            project_name VARCHAR(255),
            month INT,
            year INT
        );

        """
    hook = PostgresHook(postgres_conn_id='bt4301_postgres')
    hook.run(create_table_sql)

    df_private_transactions = pd.read_csv(data_path_private_transactions)
    df_private_rentals = pd.read_csv(data_path_private_rentals)

    insert_to_table(hook, 'private_transactions', df_private_transactions)
    insert_to_table(hook, 'private_rental', df_private_rentals)

    return

@task
def load_datagovsg(
    
)    
