from contextlib import asynccontextmanager

import pytest

from src import httpx_client


class FakeResponse:
    def __init__(self, status_code, chunks=()):
        self.status_code = status_code
        self.headers = {}
        self._chunks = list(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def aread(self):
        return b"error"

    async def aiter_lines(self):
        for chunk in self._chunks:
            yield chunk

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk.encode()


class FakeClient:
    def __init__(self, response):
        self.response = response

    def stream(self, *args, **kwargs):
        return self.response


def install_response(monkeypatch, response):
    @asynccontextmanager
    async def fake_client(**kwargs):
        yield FakeClient(response)

    monkeypatch.setattr(httpx_client.http_client, "get_streaming_client", fake_client)


@pytest.mark.asyncio
async def test_stream_callbacks_start_timer_only_after_http_200(monkeypatch):
    install_response(monkeypatch, FakeResponse(200, ["data: first"]))
    attempts = []
    started = []

    result = [item async for item in httpx_client.stream_post_async(
        "https://example.test",
        {},
        on_request_attempt=lambda: attempts.append("attempt"),
        on_response_started=lambda status: started.append(status),
    )]

    assert result == ["data: first"]
    assert attempts == ["attempt"]
    assert started == [200]


@pytest.mark.asyncio
async def test_http_429_counts_attempt_but_does_not_start_timer(monkeypatch):
    install_response(monkeypatch, FakeResponse(429))
    attempts = []
    started = []

    result = [item async for item in httpx_client.stream_post_async(
        "https://example.test",
        {},
        on_request_attempt=lambda: attempts.append("attempt"),
        on_response_started=lambda status: started.append(status),
    )]

    assert len(result) == 1
    assert result[0].status_code == 429
    assert attempts == ["attempt"]
    assert started == []
