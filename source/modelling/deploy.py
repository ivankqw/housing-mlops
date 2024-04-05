import os
import mlflow
from dotenv import load_dotenv


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
    print(f"Running docker image: {image_name}")
    os.system(f"docker run -p {port}:{mlflow_default_port} {image_name}")


def test_dev_server():
    print("Testing dev server")
    #TODO make this bandy
    raise NotImplementedError("Test not implemented")


if __name__ == "__main__":
    _ = load_dotenv("dev.env")
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
    test_dev_server()

    _ = load_dotenv("prod.env")
    model_uri = os.getenv("MODEL_URI")
    image_name = os.getenv("IMAGE_NAME")
    port = int(os.getenv("PORT"))
    assert model_uri, "MODEL_URI not found in prod environment"
    assert image_name, "IMAGE_NAME not found in prod environment"
    assert port, "PORT not found in prod environment"

    print("Building and running prod server")
    build_model_server_image(model_uri, image_name)
    run_model_server_image(image_name, port)
