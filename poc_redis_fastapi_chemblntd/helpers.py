import random
import string
from dataclasses import dataclass
from typing import Union, Iterable
import types


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


def class_with_types(name: str, bases: Iterable[object], kwds: dict[str, str]) -> type:
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
    return types.new_class(
        name, bases, {}, {k: type_annotations[v] for k, v in kwds.items()}
    )


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
