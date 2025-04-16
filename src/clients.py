from miniopy_async import Minio

from src.config import Config


class MinioClient:
    MINIO_ENDPOINT_URL: str = Config.MINIO_ENDPOINT_URL
    MINIO_ACCESS_KEY: str = Config.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY: str = Config.MINIO_SECRET_KEY

    @staticmethod
    def connect():
        return Minio(endpoint=MinioClient.MINIO_ENDPOINT_URL, access_key=MinioClient.MINIO_ACCESS_KEY,
                     secret_key=MinioClient.MINIO_SECRET_KEY, secure=False)
