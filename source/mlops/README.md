# MLOps Documentation

This directory contains the code and resources for the MLOps pipeline, covering the design, development, preparation, deployment, monitoring, and feedback loop of machine learning models.

## Lecture 07: MLOps I - Designing and Developing Models

The `experimentation` directory contains the Jupyter notebook for data exploration, feature engineering, model training, and evaluation.

Key files:
- `train.ipynb`: Notebook for training and evaluating models using MLflow for experiment tracking. Includes logic for exploration and feature engineering as well

## Lecture 08: MLOps II - Preparing for Production

The `helpers` directory contains utility functions and classes for data preprocessing and model prediction.

Key files:
- `predictor.py`: Predictor class for loading and using trained models.

The `_scalers` directory stores the trained scalers for data preprocessing.

## Lecture 09: MLOps III - Deploying to Production

The `deployment` directory contains the code for deploying the trained models.

Key files:
- `deploy.py`: CI/CD simulation script for deploying models to production. it runs a dev server, run tests, and deploy the model to production.

## Lecture 10: MLOps IV - Monitoring and Feedback Loop

The `monitoring_feedback` directory contains Jupyter notebooks for monitoring model performance and handling feedback.

Key files:
- `monitoring_feedback.ipynb` Notebook for monitoring model performance, detecting data drift, and triggering notifications.

The `tests` directory contains unit tests for the MLOps pipeline components.

## Running the Streamlit App

To run the Streamlit app and demo the MLOps pipeline:

1. Run the dev server and wait for it to start up:
   ```bash
   python run_dev_server.py
   ```

2. Run the Streamlit app:
   ```bash
   streamlit run app.py
   ```