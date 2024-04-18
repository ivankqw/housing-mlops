
# Codebase for BT4301 Project

This repository contains the codebase for the BT4301 project group 8, which focuses on DataOps and MLOps practices.

## DataOps

The `dataops` directory supports DataOps practices using Airflow and Docker Compose. It includes:

- Airflow setup with Docker Compose for Infrastructure as Code (IaC)
- DAG definitions for orchestrating data workflows
- Data extraction, transformation, and loading tasks
- Data profiling and quality reporting

Refer to the [DataOps README](./dataops/README.md) for more details.

## MLOps

The `mlops` directory covers the design, development, preparation, deployment, monitoring, and feedback loop of machine learning models. It includes:

- Model experimentation and training
- Data preprocessing and model prediction helpers
- Model deployment code
- Monitoring and feedback loop notebooks
- Tests for MLOps pipeline components

Refer to the [MLOps README](./mlops/README.md) for more details on setup and usage.

## Running the Project

To run the project:

1. Set up the DataOps environment by following the instructions in the [DataOps Setup Guide](./dataops/SETUP.md).

2. Set up the MLOps environment by following the instructions in the [MLOps Readme](./mlops/README.md).

3. Run the Streamlit app to demo the MLOps pipeline. More in the [MLOps README](./mlops/README.md).:
```bash
streamlit run app.py
```


