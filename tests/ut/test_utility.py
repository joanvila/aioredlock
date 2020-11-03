import json

import pytest
from aioredlock.utility import clean_password


def test_ignores_details_with_no_password():
    details = {"foo": "bar"}
    cleaned = clean_password(details)

    assert str(details) == cleaned


def test_cleans_details_with_password():
    details = {"foo": "bar", "password": "topsecret"}
    cleaned = clean_password(details)

    assert json.loads(cleaned.replace("'", "\"")) == {'foo': 'bar', 'password': '*******'}


def test_cleans_details_with_password_in_list():
    details = [{"foo": "bar", "password": "topsecret"}]
    cleaned = clean_password(details)

    assert json.loads(cleaned.replace("'", "\"")) == [{'foo': 'bar', 'password': '*******'}]


def test_ignores_non_dsn_string():
    details = "Hello, world."
    cleaned = clean_password(details)

    assert details == cleaned


@pytest.mark.parametrize(
    "details,protocol",
    [
        ("redis://someserver:1234/0", "redis"),
        ("redis://:topsecret@someserver:1234/0", "redis"),
        ("redis://:h8#iY60o$cqo@!39iaS&VI8tx@someserver:1234/0", "redis"),
        ("rediss://:blahpass@someserver:1234/0", "rediss"),
    ],
)
def test_cleans_dsn_string(details, protocol):
    cleaned = clean_password(details)

    assert cleaned == "{0}://:*******@someserver:1234/0".format(protocol)


def test_cleans_dsn_string_in_list():
    details = ["redis://:topsecret@someserver:1234/0"]
    cleaned = clean_password(details)

    assert cleaned == "['redis://:*******@someserver:1234/0']"
