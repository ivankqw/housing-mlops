from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


def insert_to_table(hook, table_name, df):
    print(f"Inserting {len(df)} rows into {table_name} table")
    hook.insert_rows(table=table_name, rows=df.itertuples(index=False))
    print(f"Inserted {len(df)} rows into {table_name} table")


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
):
    create_table_sql = """
        DROP TABLE IF EXISTS private_transactions;
        
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
        """
    hook = PostgresHook(postgres_conn_id='bt4301_postgres')
    hook.run(create_table_sql)

    df_private_transactions = pd.read_csv(data_path_private_transactions)

    insert_to_table(hook, 'private_transactions', df_private_transactions)

    return


@task
def load_datagovsg(
    data_path_resale_flat_transactions: str,
    data_path_hdb_information: str
):
    create_table_sql = """
        DROP TABLE IF EXISTS hdb_information;
        DROP TABLE IF EXISTS resale_flats;

        CREATE TABLE IF NOT EXISTS hdb_information (
            year_completed INT,
            multigen_sold INT,
            bldg_contract_town VARCHAR(255),
            multistorey_carpark VARCHAR(255),
            street VARCHAR(255),
            total_dwelling_units INT,
            blk_no VARCHAR(255),
            exec_sold INT,
            max_floor_lvl INT,
            residential VARCHAR(255),
            one_room_sold INT,
            precinct_pavilion VARCHAR(255),
            other_room_rental INT,
            five_room_sold INT,
            three_room_sold INT,
            commercial VARCHAR(255),
            four_room_sold INT,
            miscellaneous VARCHAR(255),
            studio_apartment_sold INT,
            two_room_rental INT,
            two_room_sold INT,
            one_room_rental INT,
            three_room_rental INT,
            market_hawker VARCHAR(255),
            _id INT NOT NULL PRIMARY KEY
        );
               
        CREATE TABLE IF NOT EXISTS resale_flats (
            town VARCHAR(255),
            flat_type VARCHAR(255),
            flat_model VARCHAR(255),
            floor_area_sqm FLOAT,
            street_name VARCHAR(255),
            resale_price FLOAT,
            month INT,
            remaining_lease VARCHAR(255),
            lease_commence_date INT,
            storey_range VARCHAR(255),
            _id INT NOT NULL PRIMARY KEY,
            block VARCHAR(255),
            year INT,
            street_name_with_block VARCHAR(255),
            postal INT,
            x_coord VARCHAR(255),
            y_coord VARCHAR(255),
            latitude VARCHAR(255),
            longitude VARCHAR(255),
            district INT
        );
        """
    hook = PostgresHook(postgres_conn_id='bt4301_postgres')
    hook.run(create_table_sql)

    df_resale_flat_transactions = pd.read_csv(
        data_path_resale_flat_transactions)
    df_hdb_information = pd.read_csv(data_path_hdb_information)

    insert_to_table(hook, 'resale_flats', df_resale_flat_transactions)
    insert_to_table(hook, 'hdb_information', df_hdb_information)
