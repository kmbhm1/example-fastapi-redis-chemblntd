import logging
import os
import random
import string
import types
import uuid
from typing import Any, Iterable, Union

import chardet
import pandas as pd
import redis
from pydantic import BaseModel
from redis_om import HashModel

# get root logger
logger = logging.getLogger(__name__)


# @dataclass
# class Attribute:
#     """
#     A class to represent an attribute of a new class.
#     """

#     name: str
#     annotation: Union[int, float, str, bool]


# a python dict for type annotations
type_annotations = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
}


def class_with_types(name: str, bases: Iterable[object], kwds: dict) -> type:
    """Create a class with custom metadata & attributes.

    Refer to https://docs.python.org/3/library/types.html?highlight=type#\
        dynamic-type-creation

    Args:
        name (str): The name of the class to create.
        bases (Iterable[object]): The base classes of the class to create.

        **kwargs: The attributes and types of the class to set.

    Returns:
        type: The class object.

    Examples:
        >>> class_with_types("Foo", (object, HashModel), [Attribute("bar", int)])
        <class '__main__.Foo'>
    """
    return types.new_class(name, bases, {}, lambda ns: ns.update(kwds))


def random_value(length: int = 10, type: str = "str") -> Union[int, float, str, bool]:
    """Generate a random value.

    Args:
        length (int, optional): The length of the random value to generate.
            Defaults to 10.
        type (str, optional): The type of the random value to generate.
            Defaults to "str".

    Returns:
        Union[int, float, str, bool]: The random value.

    Examples:
        >>> random_value(5, "str")
        'kq7b8'
        >>> random_value(5, "int")
        12345
        >>> random_value(5, "float")
        0.12345
        >>> random_value(5, "bool")
        True
    """

    if type == "str":
        return random_string(length)
    elif type == "int":
        return random_int(length)
    elif type == "float":
        return random_float(length)
    elif type == "bool":
        return random_bool()
    else:
        raise ValueError(f"Type {type} is not supported.")


def random_string(length: int = 10) -> str:
    """Generate a random string.

    Args:
        length (int, optional): The length of the random string to generate.
            Defaults to 10.

    Returns:
        str: The random string.

    Examples:
        >>> random_string(5)
        'kq7b8'
    """

    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def random_int(length: int = 10) -> int:
    """Generate a random integer.

    Args:
        length (int, optional): The length of the random integer to generate.
            Defaults to 10.

    Returns:
        int: The random integer.

    Examples:
        >>> random_int(5)
        12345
    """

    return random.randint(0, 10**length)


def random_float(length: int = 10) -> float:
    """Generate a random float.

    Args:
        length (int, optional): The length of the random float to generate.
            Defaults to 10.

    Returns:
        float: The random float.

    Examples:
        >>> random_float(5)
        0.12345
    """

    return random.random() * 10**length


def random_bool() -> bool:
    """Generate a random bool.

    Returns:
        bool: The random bool.

    Examples:
        >>> random_bool()
        True
    """

    return bool(random.getrandbits(1))


def get_proper_class_name(name: str) -> str:
    return name.strip().replace(" ", "").capitalize()


def get_base_class(model: str) -> dict:
    if model == "hash":
        return {"model": HashModel, "instance": None}
    elif model == "base":
        return {"model": BaseModel, "instance": None}
    else:
        raise ValueError(f"Model {model} is not supported.")


def remove_file(path: str) -> None:
    """Delete a file if it exists.

    Args:
        path (str): The path to the file to delete.

    Examples:
        >>> remove_file("foo.txt")
    """
    try:
        os.remove(path)
    except OSError:
        logger.warning(f"File {path} can not be removed.")
        pass


def ping_redis(r: redis.Redis) -> bool:
    """Ping the Redis database to check that it is up.

    Uses the `ping` method of the `redis` package to check that the Redis database is up.

    Returns:
        bool: True if the Redis database is up, False otherwise.

    Examples:
        >>> ping_redis()
        True
    """
    try:
        logger.info("Pinging redis db.")
        r.ping()
        logger.info("Redis db is up.")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error("Redis db is down. Exiting.")
        logger.error(e)
        return False


def flush_redis(r: redis.Redis) -> bool:
    """Flush the Redis database.

    Returns:
        bool: True if the Redis database is flushed, False is there is an Exception thrown.

    Examples:
        >>> flush_redis()
        True
    """
    try:
        logger.info("Flushing redis db.")
        r.flushdb()
        return True
    except Exception as e:
        logger.error("Error flushing redis db. Exiting.")
        logger.error(e)
        return False


def clear_ns(r: redis.Redis, ns: str):
    """
    Clears a namespace
    :param ns: str, namespace i.e your:prefix
    :return: int, cleared keys
    """
    cursor: int = -1
    ns_keys = ns + "*"
    CHUNK_SIZE = 5000
    while cursor != 0:
        cursor, keys = r.scan(cursor=cursor, match=ns_keys, count=CHUNK_SIZE)
        if keys:
            r.delete(*keys)

    return True


def temp_location(directory: str, ending: str = "csv") -> str:
    """Get the location of a temporary file."""
    return f"{directory}/{str(uuid.uuid4())}.{ending}"


def is_float(s: str) -> bool:
    """Check if a string is a float.

    Args:
        s (str): The string to check.

    Returns:
        bool: True if the string is a float, False otherwise.

    Examples:
        >>> is_float("1.0")
        True
        >>> is_float("1")
        False
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_int(s: str) -> bool:
    """Check if a string is an integer.

    Args:
        s (str): The string to check.

    Returns:
        bool: True if the string is an integer, False otherwise.

    Examples:
        >>> is_int("1")
        True
        >>> is_int("1.0")
        False
    """
    try:
        return float(s).is_integer()
    except ValueError:
        return False


def is_bool(s: str) -> bool:
    """Check if a string is a boolean.

    Args:
        s (str): The string to check.

    Returns:
        bool: True if the string is a boolean, False otherwise.

    Examples:
        >>> is_bool("True")
        True
        >>> is_bool("1")
        False
    """
    if s.lower() in ["true", "false"]:
        return True
    else:
        return False


def get_encoding(file: str) -> str:
    """Get the encoding of a file.

    Args:
        file (str): The path to the file to get the encoding of.

    Returns:
        str: The encoding of the file.

    Examples:
        >>> get_encoding("foo.txt")
        "utf-8"
    """
    try:
        with open(file, "rb") as f:
            rawdata = f.read(10000)
    except Exception as e:
        logger.error(f"Error reading file {file}.")
        logger.error(e)
        return "utf-8"

    try:
        return chardet.detect(rawdata)["encoding"]
    except Exception as e:
        logger.error(f"Error detecting encoding of file {file}.")
        logger.error(e)
        return "utf-8"


def geuss_schema(df: pd.DataFrame, sample_size: int = 25) -> dict[str, type[Any]]:
    """Geuss the schema of a pandas DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to geuss the schema of.

    Returns:
        dict[str, Type]: A dictionary of the column names and their associated types.

    Examples:
        >>> df = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        >>> geuss_schema(df)
        {"a": int, "b": float}
    """
    schema: dict[str, type[Any]] = {}
    first_rows = df.head(sample_size)

    for col in first_rows.columns:
        c = first_rows[col].to_list()

        if first_rows[col].dtype == "int64" or all(is_int(e) for e in c):
            schema[col] = int
        elif first_rows[col].dtype == "float64" or all(is_float(e) for e in c):
            schema[col] = float
        elif first_rows[col].dtype == "object":
            schema[col] = str
        elif first_rows[col].dtype == "bool" or all(is_bool(e) for e in c):
            schema[col] = bool
        else:
            schema[col] = str

    return schema


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the column names of a pandas DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to clean the column names of.

    Returns:
        pd.DataFrame: The pandas DataFrame with the cleaned column names.

    Examples:
        >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [1.0, 2.0, 3.0]})
        >>> clean_column_names(df)
           a    b
        0  1  1.0
        1  2  2.0
        2  3  3.0
    """
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("%", "pct")
    )

    return df


def add_refresh_attributes(d: dict[str, Any]) -> dict:
    data = {}
    for col, t in d.items():
        data[col] = {"type": t, "index": False, "full_text_search": False}

    return data


def fill_na(df: pd.DataFrame, schema: dict[str, str] | None) -> pd.DataFrame:
    if schema is not None:
        for col, t in schema.items():
            if t == "int":
                df[col] = df[col].fillna(0)
            elif t == "float":
                df[col] = df[col].fillna(0.0)
            elif t == "bool":
                df[col] = df[col].fillna(False)
            else:
                df[col] = df[col].fillna("")

    return df
