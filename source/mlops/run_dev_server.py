from dotenv import load_dotenv 
import os 
from deploy import build_model_server_image, run_model_server_image


if __name__ == "__main__":
    _ = load_dotenv("../dev.env")
    model_uri = os.getenv("MODEL_URI")
    image_name = os.getenv("IMAGE_NAME")
    port = int(os.getenv("PORT"))

    build_model_server_image(model_uri, image_name)
    run_model_server_image(image_name, port)
