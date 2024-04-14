## Simple example to build and run test server, then test the model


1. Build the docker image with the model (don't need to start mlflow tracking server)
```bash
mlflow models build-docker -m mlartifacts/0/14d46f61c8bd43d48c364d72029e90b4/artifacts/model -n xgboost_dev --enable-mlserver
```

2. Run the docker image
```bash
docker run -p 8002:8080 xgboost_dev
```

3. Test the model
```bash
python preprocess_and_predict.py --port 8002 --flat_type 3_ROOM --storey_range 01_TO_03 --flat_model Improved --district 20.0 --month 7 --year 2021 --remaining_lease_years 95 --remaining_lease_months 0 --floor_area_sqm 70   
```