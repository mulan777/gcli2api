import asyncio

import pytest

from src.delayed_hedge import hedge_stream


class ProbeStream:
    def __init__(self, items, first_delay=0, gate=None):
        self.items = list(items)
        self.first_delay = first_delay
        self.gate = gate
        self.index = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index == 0:
            if self.gate is not None:
                await self.gate.wait()
            if self.first_delay:
                await asyncio.sleep(self.first_delay)
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item

    async def aclose(self):
        self.closed = True


class RaisingStream(ProbeStream):
    async def __anext__(self):
        raise RuntimeError("socket failed before response")


async def collect(stream):
    return [item async for item in stream]


@pytest.mark.asyncio
async def test_primary_before_delay_does_not_start_backup():
    primary = ProbeStream(["primary-1", "primary-2"])
    backup_started = False
    events = []

    def backup_factory():
        nonlocal backup_started
        backup_started = True
        return ProbeStream(["backup"])

    result = await collect(hedge_stream(
        lambda: primary,
        backup_factory,
        delay_seconds=0.05,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
    ))

    assert result == ["primary-1", "primary-2"]
    assert backup_started is False
    assert events == []


@pytest.mark.asyncio
async def test_timeout_starts_only_after_primary_http_200_event():
    response_started = asyncio.Event()
    primary_gate = asyncio.Event()
    backup_started = asyncio.Event()

    def backup_factory():
        backup_started.set()
        return ProbeStream(["backup"])

    task = asyncio.create_task(collect(hedge_stream(
        lambda: ProbeStream(["primary"], gate=primary_gate),
        backup_factory,
        delay_seconds=0.03,
        is_success=lambda item: not isinstance(item, Exception),
        primary_started=response_started,
    )))

    await asyncio.sleep(0.06)
    assert backup_started.is_set() is False

    response_started.set()
    await asyncio.wait_for(backup_started.wait(), timeout=0.1)
    assert await task == ["backup"]


@pytest.mark.asyncio
async def test_backup_wins_after_primary_first_item_timeout():
    response_started = asyncio.Event()
    response_started.set()
    primary = ProbeStream(["primary"], first_delay=0.2)
    backup = ProbeStream(["backup-1", "backup-2"])
    events = []

    result = await collect(hedge_stream(
        lambda: primary,
        lambda: backup,
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
        primary_started=response_started,
    ))

    assert result == ["backup-1", "backup-2"]
    assert events == ["triggered", "backup_won"]
    assert primary.closed is True


@pytest.mark.asyncio
async def test_primary_can_win_after_backup_starts():
    primary = ProbeStream(["primary"], first_delay=0.04)
    backup = ProbeStream(["backup"], first_delay=0.2)
    events = []

    result = await collect(hedge_stream(
        lambda: primary,
        lambda: backup,
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
    ))

    assert result == ["primary"]
    assert events == ["triggered", "primary_won"]
    assert backup.closed is True


@pytest.mark.asyncio
async def test_backup_error_does_not_beat_late_primary_success():
    primary = ProbeStream(["primary"], first_delay=0.06)
    backup_error = RuntimeError("backup failed")
    backup = ProbeStream([backup_error])
    events = []

    result = await collect(hedge_stream(
        lambda: primary,
        lambda: backup,
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
    ))

    assert result == ["primary"]
    assert events == ["triggered", "primary_won"]


@pytest.mark.asyncio
async def test_both_errors_return_primary_error():
    primary_error = RuntimeError("primary failed")
    backup_error = RuntimeError("backup failed")
    primary = ProbeStream([primary_error], first_delay=0.04)
    backup = ProbeStream([backup_error])

    result = await collect(hedge_stream(
        lambda: primary,
        lambda: backup,
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
    ))

    assert result == [primary_error]


@pytest.mark.asyncio
async def test_primary_task_exception_allows_backup_to_rescue():
    events = []
    result = await collect(hedge_stream(
        lambda: RaisingStream([]),
        lambda: ProbeStream(["backup"]),
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
    ))

    assert result == ["backup"]
    assert events == ["triggered", "backup_won", "rescued"]


@pytest.mark.asyncio
async def test_empty_items_do_not_count_as_first_output():
    primary = ProbeStream(["", "primary"], first_delay=0.2)
    backup = ProbeStream(["backup"])
    events = []

    result = await collect(hedge_stream(
        lambda: primary,
        lambda: backup,
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
        is_ignorable=lambda item: item == "",
        on_event=events.append,
    ))

    assert result == ["backup"]
    assert events == ["triggered", "backup_won"]


@pytest.mark.asyncio
async def test_empty_primary_stream_is_rescued_by_backup():
    primary = ProbeStream([])
    backup = ProbeStream(["backup"])
    events = []

    result = await collect(hedge_stream(
        lambda: primary,
        lambda: backup,
        delay_seconds=0.02,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
    ))

    assert result == ["backup"]
    assert events == ["triggered", "backup_won", "rescued"]


@pytest.mark.asyncio
async def test_simultaneous_success_prefers_primary():
    gate = asyncio.Event()
    primary = ProbeStream(["primary"], gate=gate)
    backup = ProbeStream(["backup"], gate=gate)
    events = []
    backup_created = asyncio.Event()

    def backup_factory():
        backup_created.set()
        return backup

    task = asyncio.create_task(collect(hedge_stream(
        lambda: primary,
        backup_factory,
        delay_seconds=0,
        is_success=lambda item: not isinstance(item, Exception),
        on_event=events.append,
    )))
    await asyncio.wait_for(backup_created.wait(), timeout=0.1)
    gate.set()

    assert await task == ["primary"]
    assert events == ["triggered", "primary_won"]
