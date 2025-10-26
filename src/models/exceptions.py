class RetryableHTTPStatusException(Exception):
    def __init__(self, url: str, status: int):
        super().__init__(f"Retryable HTTP status {status} for {url}")
        self.url = url
        self.status = status