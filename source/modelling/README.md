## train.ipynb: train with mlflow, then deploy the model as a docker image
```bash
pip install mlflow
```

1. Run the MLFlow server on ur terminal

```bash
mlflow server --host 127.0.0.1 --port 8000
```

2. In one of the cells, the tracking url is set to localhost 
    
```python   
mlflow.set_tracking_uri('http://localhost:8000')
``` 

3. Run the training as per the notebook

4. Go to the MLFlow UI at http://localhost:8000 and you can see the runs and the metrics

5. Choose a run and copy the run_id, then run the last cell in the notebook to get the model as a docker image

6. Run the docker image with the run_id as an argument

```bash
docker run -p 5000:5000 <image_id> <run_id>
```

**For ease of development, you can run the shell script `model_server.sh` to start the MLFlow server and the docker image.**

## Testing the served model

**You should have the docker image from step 6 running for this to work**

> Refer to `preprocess_and_predict.py`. This script preprocesses the input data and sends it to the model server for prediction, then inverse transforms the prediction to get the original values.

Example usage:
    
```bash
python preprocess_and_predict.py --flat_type 3_ROOM --storey_range 01_TO_03 --flat_model Improved --district 20.0 --month 7 --year 2021 --remaining_lease_years 95 --remaining_lease_months 0 --floor_area_sqm 70   
```
