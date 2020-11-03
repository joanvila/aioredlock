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

    assert json.loads(cleaned) == {'foo': 'bar', 'password': '*******'}


def test_cleans_details_with_password_in_list():
    details = [{"foo": "bar", "password": "topsecret"}]
    cleaned = clean_password(details)

    assert json.loads(cleaned) == [{'foo': 'bar', 'password': '*******'}]


def test_ignores_non_dsn_string():
    details = "Hello, world."
    cleaned = clean_password(details)

    assert details == cleaned


@pytest.mark.parametrize(
    "details",
    [
        "redis://someserver:1234/0",
        "redis://:topsecret@someserver:1234/0",
        "redis://:h8#iY60o$cqo@!39iaS&VI8tx@someserver:1234/0",
    ],
)
def test_cleans_dsn_string(details):
    cleaned = clean_password(details)

    assert cleaned == "redis://:*******@someserver:1234/0"


def test_cleans_dsn_string_in_list():
    details = ["redis://:topsecret@someserver:1234/0"]
    cleaned = clean_password(details)

    assert cleaned == "['redis://:*******@someserver:1234/0']"
