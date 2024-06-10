from bs4 import Tag



def parse_object(fields: dict[str, callable], element: Tag | str) -> dict[str, str | int]:
    return {field: fields[field](element) for field in fields}

