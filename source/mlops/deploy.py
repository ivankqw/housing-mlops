import os
import mlflow
from dotenv import load_dotenv
from predictor import Predictor


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
    os.system(f"docker run -d --name {container_name} -p {port}:{mlflow_default_port} {image_name}")


def test_dev_server(port: int) -> bool:
    print("Testing dev server")

    test_files = ["test_data_type.csv",
                  "test_extreme_values.csv", "test_null_values.csv"]
    predictor = Predictor(8002)
    for test_file in test_files:
        try:
            predictor.predict_csv(test_file)
        except Exception as e:
            print(f"Failed to predict for {test_file}: {e}")
            raise e
    return True


if __name__ == "__main__":
    _ = load_dotenv("../dev.env")
    model_uri = os.getenv("MODEL_URI")
    image_name = os.getenv("IMAGE_NAME")
    port = int(os.getenv("PORT"))
    assert model_uri, "MODEL_URI not found in dev environment"
    assert image_name, "IMAGE_NAME not found in dev environment"
    assert port, "PORT not found in dev environment"
    print(model_uri, image_name, port)

    print("Building and running dev server")
    build_model_server_image(model_uri, image_name)
    run_model_server_image(image_name, port)
    test_dev_server(port=port)

    # _ = load_dotenv("../prod.env")
    # model_uri = os.getenv("MODEL_URI")
    # image_name = os.getenv("IMAGE_NAME")
    # port = int(os.getenv("PORT"))
    # assert model_uri, "MODEL_URI not found in prod environment"
    # assert image_name, "IMAGE_NAME not found in prod environment"
    # assert port, "PORT not found in prod environment"

    # print("Building and running prod server")
    # build_model_server_image(model_uri, image_name)
    # run_model_server_image(image_name, port)
