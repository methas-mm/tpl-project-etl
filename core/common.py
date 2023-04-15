from typing import Any, TypeVar, Type, cast

T = TypeVar("T")

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def gen_name(prefix, date, suffix):
    name = ""
    if len(prefix) == 0:
        name = str(prefix) % (date, "")
    else:
        name = str(prefix) % (date, suffix)
    return name


def from_str(obj: Any) -> str:
    assert isinstance(obj, str)
    return obj


def from_int(obj: Any) -> int:
    assert isinstance(obj, int) and not isinstance(obj, bool)
    return obj


def from_bool(obj: Any) -> bool:
    assert isinstance(obj, bool)
    return obj


def from_dict(obj: Any) -> dict:
    assert isinstance(obj, dict)
    return obj


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()
