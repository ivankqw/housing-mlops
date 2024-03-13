from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 4),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

mock_order_data = {
    'order_id': 0,
    'customer_id': 123,
    'amount': 100.0,
    'status': 'completed'
}


@dag(dag_id='ecommerce_etl_taskflow', default_args=default_args, schedule=None, catchup=False, tags=['tutorial5'])
def tutorial_taskflow_api_etl():
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
        hook = PostgresHook(postgres_conn_id='postgres_t5')
        hook.run(create_table_sql, autocommit=True)

    @task
    def extract():
        # by default, any operator that return a value will push it to xcom
        return mock_order_data

    @task
    def transform(order_data_dict: dict):
        order_data_dict['amount'] = order_data_dict['amount'] * \
            1.1  # Apply a tax
        return order_data_dict

    @task
    def load(order_data_dict: dict):
        load_sql = """
        INSERT INTO orders (order_id, customer_id, amount, status)
        VALUES (%(order_id)s, %(customer_id)s, %(amount)s, %(status)s);
        """
        # Use the Airflow PostgresHook to execute the SQL
        hook = PostgresHook(postgres_conn_id='postgres_t5')
        hook.run(load_sql, parameters=order_data_dict, autocommit=True)

    # task dependencies are defined in a straightforward way
    create_table()
    order_data = extract()
    transformed_data = transform(order_data)
    load(transformed_data)


tutorial_etl_dag = tutorial_taskflow_api_etl()
