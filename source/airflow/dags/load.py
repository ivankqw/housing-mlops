from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from typing import *

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

    # use postgres to load df_cpi pandas dataframe to cpi table
    hook.insert_rows(table='cpi', rows=df_cpi.itertuples(index=False))
    
    return


@task
def load_singstat(
    data_path_private_transactions: str,
    data_path_private_rentals: str,
    data_path_planning_decisions: str,
):
