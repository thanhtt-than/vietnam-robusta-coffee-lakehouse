import os

from dotenv import load_dotenv
from minio import Minio


load_dotenv()


def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    return client


def get_bucket_name():
    return os.getenv("MINIO_BUCKET", "coffee-lakehouse")