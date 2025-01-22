from src.patterns.mixin.data_processing import ProcessingMixin
from src.patterns.mixin.file_storage import FileStorageMixin
from src.patterns.mixin.http_client import HTTPClientMixin


class Parser(ProcessingMixin, FileStorageMixin, HTTPClientMixin):
    pass