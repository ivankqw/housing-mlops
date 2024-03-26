from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from uranus import extract_ura_data_task
from singstat import extract_cpi_task
from datagovsg import extract_datagovsg_data_task
from transform_datagovsg import transform_districts_task, transform_resale_flat_transactions_task, transform_rental_flats_task

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 4),
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

mock_order_data = {
    'order_id': 0,
    'customer_id': 123,
    'amount': 100.0,
    'status': 'completed'
}

@dag(dag_id='bt4301_dag', default_args=default_args, schedule=None, catchup=False, tags=['twk'])
def property_pipeline():
    @task
    def create_table():
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount FLOAT NOT NULL,
            status VARCHAR(50) NOT NULL
        );
        """
        # Use the Airflow PostgresHook to execute the SQL
        hook = PostgresHook(postgres_conn_id='bt4301_postgres')
        hook.run(create_table_sql, autocommit=True)


    # task dependencies are defined in a straightforward way
    data_path_cpi = extract_cpi_task()
    data_path_private_transactions, data_path_private_rental, data_path_planning_decisions = extract_ura_data_task()
    
    datagov_dict = extract_datagovsg_data_task()
    districts_transformed_path = transform_districts_task()
    data_path_resale_flats = transform_resale_flat_transactions_task(datagov_dict, districts_transformed_path)
    data_path_rental_flats = transform_rental_flats_task(datagov_dict, districts_transformed_path)


property_pipeline()
