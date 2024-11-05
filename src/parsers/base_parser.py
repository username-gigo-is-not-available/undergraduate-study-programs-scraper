from src.patterns.mixin import StorageMixin, ProcessingMixin
from src.patterns.common import HTTPClientMixin


class Parser(ProcessingMixin, StorageMixin, HTTPClientMixin):
    pass