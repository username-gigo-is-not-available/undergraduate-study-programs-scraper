import logging

from bs4 import Tag

from src.patterns.decorator.resolve import prepend_base_url
from src.patterns.decorator.validate import validate_url


class FieldParser:
    def __init__(self,
                 field_name: str,
                 selector: str | list[str],
                 element: Tag,
                 function: callable
                 ):
        self.field_name: str = field_name
        self.selector: str | list[str] = selector
        self.element: Tag = element
        self.function: callable = function

    def parse_text_field(self, field_type: type = str) -> type:
        try:
            return field_type(self.element.select_one(self.selector).text.strip())
        except (ValueError, TypeError, AttributeError):
            logging.warning(f"Failed to parse {field_type.__name__} from field {self.field_name}")
            return field_type()

    @validate_url
    @prepend_base_url
    def parse_url_field(self) -> str:
        return self.element.select_one(self.selector)['href']

    def check_if_fields_exists(self) -> bool:
        return all([self.element.select_one(selector) for selector in self.selector])

    def __call__(self) -> str | int | bool:
        return self.function(self)


