from dotenv import load_dotenv 
import sys
import os 

sys.path.append("deployment")
from deploy import build_model_server_image, run_model_server_image


if __name__ == "__main__":
    _ = load_dotenv("../../dev.env")
    model_uri = os.getenv("MODEL_URI")
    image_name = os.getenv("DEV_IMAGE_NAME")
    port = int(os.getenv("DEV_PORT"))

    build_model_server_image(model_uri, image_name)
    run_model_server_image(image_name, port)
