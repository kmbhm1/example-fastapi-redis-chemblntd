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
