# DataOps Support

This directory contains the necessary components to support DataOps practices using Airflow and Docker Compose.

## Airflow Setup with Docker Compose

The following directories are used to set up Airflow using Docker Compose, enabling Infrastructure as Code (IaC):

- `config`: Contains Airflow configuration files.
- `dags`: Contains Airflow DAG definitions for orchestrating data workflows.
- `logs`: Stores Airflow logs. (NA)
- `plugins`: Contains custom Airflow plugins. (NA)

The `docker-compose.yml` file defines the services and their configurations for running Airflow.

## DAG Files

- `dag.py`: Defines the main Airflow DAG that orchestrates the property pipeline. It includes tasks for data extraction, transformation, loading, and sending data profiling emails.

- `cfg.py`: Contains configuration variables used across the DAG files, such as file paths and API URLs.

- `dataops.py`: Provides utility functions for data quality reporting and generating charts for numerical and categorical columns.

- `email_dataprofiling.py`: Implements tasks for generating data profiling reports and sending them via email.

- `extract_datagovsg.py`: Defines tasks for extracting data from the Data.gov.sg API, including resale flat transactions and HDB property information.

- `extract_singstat.py`: Defines a task for extracting CPI data from the SingStat API.

- `extract_ura.py`: Defines a task for extracting private property transaction data from the URA API.

- `load.py`: Defines tasks for loading the extracted and transformed data into a PostgreSQL database.

- `transform_datagovsg.py`: Defines tasks for transforming the extracted Data.gov.sg data, including district transformation and resale flat data processing.

- `transform_ura.py`: Defines a task for transforming the extracted URA private property transaction data.

These DAG files work together to define and orchestrate the data pipeline, handling data extraction from various sources, transformation, loading into a database, and generating data profiling reports.

The `charts` directory contains Jupyter notebooks and scripts for exploratory data analysis and visualization. These notebooks help gain insights into the datasets and guide feature engineering decisions.

## DataOps Practices

The setup in this directory supports key DataOps practices:

- **Infrastructure as Code**: Docker Compose allows defining and managing the Airflow infrastructure as code.
- **Data Orchestration**: Airflow DAGs enable orchestration of data ingestion, transformation, and processing workflows.
- **Automation**: Airflow automates the execution of data pipelines on a scheduled or event-driven basis.
- **Monitoring**: Airflow provides monitoring capabilities to track the status and performance of data workflows.

