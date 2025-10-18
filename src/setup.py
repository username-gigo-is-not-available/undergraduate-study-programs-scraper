import os

from dotenv import load_dotenv

load_dotenv('../.env')
ENVIRONMENT_VARIABLES: dict[str, str] = dict(os.environ)
