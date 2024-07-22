import logging
from functools import wraps

from static import ENVIRONMENT_VARIABLES


def validate_env_variables(func):
    @wraps(func)
    def wrapper_validate_env_variables(*args, **kwargs):
        logging.info("Validating environment variables...")
        for variable_name, variable_value in ENVIRONMENT_VARIABLES.items():
            if not variable_value:
                raise RuntimeError(f"{variable_name} is not set in the environment variables file!")
        logging.info("Environment variables are valid!")
        return func(*args, **kwargs)

    return wrapper_validate_env_variables
