from src.patterns.mixin import FileStorageMixin, ProcessingMixin
from src.patterns.common import HTTPClientMixin


class Parser(ProcessingMixin, FileStorageMixin, HTTPClientMixin):
    pass