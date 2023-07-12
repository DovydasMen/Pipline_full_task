from typing import Union, Dict
import random
from random_word import RandomWords


def get_db_name() -> str:
    db_name = input("Please select your's db name: ")
    return db_name


def get_collection_name() -> str:
    collection_name = input("Please select your's collections name: ")
    return collection_name


def get_field_name() -> str:
    field_name = input("Please specify fields name: ")
    return field_name


def get_field_type() -> str:
    print("Possible options - str, int, float, bool")
    field_type = input("Please select input type from above listed types: ")
    return field_type


def get_int_min_value() -> int:
    min_value = int(input("Please select integral value from where to start: "))
    return min_value


def get_int_max_value() -> int:
    max_value = int(input("Please select integral value where to stop: "))
    return max_value


def get_float_min_value() -> float:
    min_value = float(input("Please select float value from where to start: "))
    return min_value


def get_float_max_value() -> float:
    max_value = float(input("Please select float value where to stop: "))
    return max_value


def get_field_counts() -> int:
    entries_count = int(
        input("Please specify how many diffrent line you want to insert in database: ")
    )
    return entries_count


def get_entries_count() -> int:
    entries_count = int(
        input("Please specify how many entries you want to insert in database: ")
    )
    return entries_count


def get_generated_random_word() -> str:
    word = RandomWords()
    return word.get_random_word()


def get_template(entries_count: int) -> Dict[str, Union["str", "int", "float", "bool"]]:
    template = {}
    rows = entries_count
    while rows > 0:
        field_name = get_field_name()
        field_type = get_field_type()
        if field_type == "str":
            template.update({field_name: "str"})
            rows = rows - 1
        if field_type == "int":
            template.update(
                {field_name: ["int", get_int_min_value(), get_int_max_value()]}
            )
            rows = rows - 1
        if field_type == "float":
            template.update(
                {field_name: ["float", get_float_min_value(), get_float_max_value()]}
            )
            rows = rows - 1
        if field_type == "bool":
            template.update({field_name: ["bool", True, False]})
            rows = rows - 1
    return template


def random_values_generator(template: Dict[str, Union[str, int, float]]) -> None:
    document = {}
    for key, value in template.items():
        if value == "str":
            document.update({key: get_generated_random_word()})
        if value[0] == "int":
            document.update({key: random.randint(value[1], value[2])})
        if value[0] == "float":
            document.update({key: random.uniform(value[1], value[2])})
        if value[0] == "bool":
            document.update({key: random.choice([value[1], value[2]])})
    return document
