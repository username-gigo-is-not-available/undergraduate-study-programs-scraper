from miniopy_async import Minio

from src.configurations import StorageConfiguration


class MinioClient:
    MINIO_ENDPOINT_URL: str = StorageConfiguration.MINIO_ENDPOINT_URL
    MINIO_ACCESS_KEY: str = StorageConfiguration.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY: str = StorageConfiguration.MINIO_SECRET_KEY

    @staticmethod
    def connect():
        return Minio(endpoint=MinioClient.MINIO_ENDPOINT_URL, access_key=MinioClient.MINIO_ACCESS_KEY,
                     secret_key=MinioClient.MINIO_SECRET_KEY, secure=False)
