"""The HTTP client: key handling, backoff, retries and error mapping.

The client is driven with ``requests.request`` patched to answer from the fake, the way
Apollo's client tests do: no network.
"""

import pytest
from fake_stack_overflow import FakeStackExchange, serve_over_http

from cognee_connector_stack_overflow.source import (
    BASE_URL,
    StackOverflowAPIError,
    StackOverflowAuthError,
    StackOverflowClient,
    StackOverflowRateLimitedError,
)

KEY = "secret-stackapps-key-123"


class _HTTPResponse:
    def __init__(self, status: int, payload):
        self.status_code = status
        self._payload = payload
        self.headers = {}

    def json(self):
        return self._payload


def _client(**kwargs) -> StackOverflowClient:
    return StackOverflowClient(KEY, **kwargs)


def test_the_key_is_sent_as_a_query_parameter(monkeypatch):
    fake, keys = FakeStackExchange(), []
    fake.add_question(101, "A question?", tags=["python"])
    monkeypatch.setattr("requests.request", serve_over_http(fake, keys=keys))

    envelope = _client().request("GET", "/questions", params={"site": "stackoverflow"})

    assert envelope["items"][0]["question_id"] == 101
    assert keys == [KEY]


def test_a_missing_key_sends_no_key_parameter(monkeypatch):
    fake, keys = FakeStackExchange(), []
    monkeypatch.setattr("requests.request", serve_over_http(fake, keys=keys))

    StackOverflowClient().request("GET", "/questions", params={"site": "stackoverflow"})

    assert keys == [None]


def test_an_answer_with_backoff_is_honored_before_the_next_request(monkeypatch):
    fake = FakeStackExchange()
    fake.backoff = 2
    fake.add_question(101, "A question?", tags=["python"])
    monkeypatch.setattr("requests.request", serve_over_http(fake))
    slept: list[float] = []
    client = StackOverflowClient(KEY, sleep=slept.append)

    client.request("GET", "/questions", params={"site": "stackoverflow"})
    assert slept == []
    client.request("GET", "/questions", params={"site": "stackoverflow"})

    assert slept == [2.0]


def test_a_5xx_is_retried_then_read_like_any_other_answer(monkeypatch):
    fake = FakeStackExchange()
    fake.add_question(101, "A question?", tags=["python"])
    serve = serve_over_http(fake)
    calls = {"n": 0}
    slept: list[float] = []

    def flaky(method, url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return _HTTPResponse(500, {})
        return serve(method, url, **kwargs)

    monkeypatch.setattr("requests.request", flaky)
    envelope = StackOverflowClient(KEY, sleep=slept.append).request(
        "GET", "/questions", params={"site": "stackoverflow"}
    )

    assert envelope["items"][0]["question_id"] == 101
    assert calls["n"] == 2
    assert slept, "a retried attempt backs off before the next one"


def test_a_network_error_raises_an_api_error_with_the_cause_type(monkeypatch):
    def offline(*args, **kwargs):
        import requests

        raise requests.ConnectionError("boom")

    monkeypatch.setattr("requests.request", offline)

    with pytest.raises(StackOverflowAPIError, match="ConnectionError"):
        _client().request("GET", "/questions", params={"site": "stackoverflow"})


@pytest.mark.parametrize("status", [301, 302])
def test_a_redirect_is_an_error_and_is_not_followed(monkeypatch, status):
    monkeypatch.setattr("requests.request", lambda *a, **k: _HTTPResponse(status, {"items": []}))

    with pytest.raises(StackOverflowAPIError, match="redirect"):
        _client().request("GET", "/questions", params={"site": "stackoverflow"})


def test_a_401_raises_an_auth_error(monkeypatch):
    monkeypatch.setattr("requests.request", lambda *a, **k: _HTTPResponse(401, {}))

    with pytest.raises(StackOverflowAuthError):
        _client().request("GET", "/questions", params={"site": "stackoverflow"})


def test_a_429_raises_a_rate_limited_error(monkeypatch):
    monkeypatch.setattr("requests.request", lambda *a, **k: _HTTPResponse(429, {}))

    with pytest.raises(StackOverflowRateLimitedError):
        _client().request("GET", "/questions", params={"site": "stackoverflow"})


def test_a_throttle_violation_envelope_raises_a_rate_limited_error(monkeypatch):
    monkeypatch.setattr(
        "requests.request",
        lambda *a, **k: _HTTPResponse(400, {"error_id": 502, "error_message": "too fast"}),
    )

    with pytest.raises(StackOverflowRateLimitedError):
        _client().request("GET", "/questions", params={"site": "stackoverflow"})


def test_an_error_envelope_carries_no_key_and_no_provider_text(monkeypatch):
    monkeypatch.setattr(
        "requests.request",
        lambda *a, **k: _HTTPResponse(400, {"error_id": 400, "error_message": KEY}),
    )

    with pytest.raises(StackOverflowAPIError) as exc_info:
        _client().request("GET", "/questions", params={"site": "stackoverflow", "key": KEY})

    assert KEY not in str(exc_info.value)
    assert "error_id 400" in str(exc_info.value)


def test_quota_remaining_is_read_from_every_answer(monkeypatch):
    fake = FakeStackExchange()
    fake.quota_remaining = 4242
    monkeypatch.setattr("requests.request", serve_over_http(fake))
    client = _client()

    assert client.quota_remaining is None
    client.request("GET", "/questions", params={"site": "stackoverflow"})
    assert client.quota_remaining == 4242


@pytest.mark.parametrize("key", ["has a space", "תקין", ""])
def test_a_malformed_key_fails_at_construction(key):
    if key == "":
        client = StackOverflowClient(key)
        assert client.quota_remaining is None
    else:
        with pytest.raises(ValueError, match="invalid format"):
            StackOverflowClient(key)


def test_repr_redacts_the_key():
    assert KEY not in repr(_client())
