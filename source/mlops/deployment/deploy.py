import sys
import os
import mlflow
from dotenv import load_dotenv
import time
sys.path.append("../")
from helpers.predictor import Predictor

# create a global predictor object
predictor = Predictor(8002,
                      data_path='../../../data/resale_flats_transformed.csv',
                      cpi_path='../../../data/cpi_with_lag_sma_ema.csv',
                      sibor_path='../../../data/sibor_sora.csv',
                      feature_scaler_path='../_scalers/feature_scaler.save',
                      target_scaler_path='../_scalers/target_scaler.save')

tests = [os.path.join('../tests', x) for x in os.listdir('../tests')]


def build_model_server_image(model_uri: str, image_name: str):
    print(f"Building docker image: {image_name}")
    try:
        mlflow.models.build_docker(
            model_uri=model_uri,
            name=image_name,
            enable_mlserver=True,
        )
    except Exception as e:
        print(f"Failed to build docker image: {e}")
        raise e


def run_model_server_image(image_name: str, port: int, mlflow_default_port: int = 8080):
    container_name = f"{image_name}-container"
    os.system(f"docker stop {container_name}")
    os.system(f"docker rm {container_name}")
    print(f"Running docker image: {image_name}")
    os.system(
        f"docker run -d --name {container_name} -p {port}:{mlflow_default_port} {image_name}")
    print("Waiting for server to start")
    time.sleep(10)


def test_dev_server(port: int, predictor: Predictor = predictor, test_files: list = tests) -> bool:
    print("Testing dev server")

    for test_file in test_files:
        try:
            predictor.predict_csv(test_file)
            time.sleep(5)
        except Exception as e:
            print(f"Failed to predict for {test_file}: {e}")
            raise e
    return True


if __name__ == "__main__":
    _ = load_dotenv("../../dev.env")
    # assuming artifact has been logged to MLflow and is the latest one that triggered the CI/CD pipeline
    model_uri = os.getenv("MODEL_URI") 

    image_name = os.getenv("DEV_IMAGE_NAME")
    port = int(os.getenv("DEV_PORT"))
    assert model_uri, "MODEL_URI not found in dev environment"
    assert image_name, "IMAGE_NAME not found in dev environment"
    assert port, "PORT not found in dev environment"
    print(f"TEST ENV: {model_uri}, {image_name}, {port}")

    print("Building and running dev server")
    build_model_server_image(model_uri, image_name)
    run_model_server_image(image_name, port)
    test_dev_server(port=port)

    image_name = os.getenv("PROD_IMAGE_NAME")
    port = int(os.getenv("PROD_PORT"))
    assert model_uri, "MODEL_URI not found in prod environment"
    assert image_name, "IMAGE_NAME not found in prod environment"
    assert port, "PORT not found in prod environment"
    print(f"PROD ENV: {model_uri}, {image_name}, {port}")

    print("Building and running prod server")
    build_model_server_image(model_uri, image_name)
    run_model_server_image(image_name, port)
