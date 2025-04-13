from miniopy_async import Minio

from src.config import Config


class MinioClient:

    @staticmethod
    def connect():
        return Minio(Config.MINIO_ENDPOINT_URL, access_key=Config.MINIO_ACCESS_KEY, secret_key=Config.MINIO_SECRET_KEY, secure=False)
