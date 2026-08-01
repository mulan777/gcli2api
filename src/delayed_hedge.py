"""Delayed hedging for async streams."""

import asyncio
import inspect
from contextlib import suppress
from typing import Any, AsyncIterator, Callable, Optional


_STREAM_EXHAUSTED = object()


async def _close_stream(stream: Optional[AsyncIterator[Any]]) -> None:
    if stream is None:
        return
    close = getattr(stream, "aclose", None)
    if close is not None:
        with suppress(Exception):
            await close()


async def _cancel_task(task: Optional[asyncio.Task]) -> None:
    if task is None or task.done():
        return
    task.cancel()
    with suppress(asyncio.CancelledError, Exception):
        await task


async def _first_decisive_item(
    stream: AsyncIterator[Any],
    is_ignorable: Callable[[Any], bool],
) -> Any:
    async for item in stream:
        if not is_ignorable(item):
            return item
    return _STREAM_EXHAUSTED


def _task_result(task: asyncio.Task) -> Any:
    try:
        return task.result()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return exc


def is_empty_stream_item(item: Any) -> bool:
    if isinstance(item, bytes):
        text = item.decode("utf-8", errors="ignore").strip()
    elif isinstance(item, str):
        text = item.strip()
    else:
        return False
    return text in ("", "data:", "data: [DONE]", "[DONE]")


async def hedge_stream(
    primary_factory: Callable[[], AsyncIterator[Any]],
    backup_factory: Callable[[], AsyncIterator[Any]],
    *,
    delay_seconds: float,
    is_success: Callable[[Any], bool],
    is_ignorable: Optional[Callable[[Any], bool]] = None,
    on_event: Optional[Callable[[str], Any]] = None,
    primary_started: Optional[asyncio.Event] = None,
):
    """Yield the first successful stream after a delayed backup launch."""
    primary = primary_factory()
    backup: Optional[AsyncIterator[Any]] = None
    ignore = is_ignorable or (lambda item: False)
    primary_task = asyncio.create_task(_first_decisive_item(primary, ignore))
    backup_task: Optional[asyncio.Task] = None
    started_task: Optional[asyncio.Task] = None
    primary_error = None
    backup_error = None
    primary_failed = False
    event_tasks: list[asyncio.Task] = []

    def emit(event: str) -> None:
        if on_event is not None:
            result = on_event(event)
            if inspect.isawaitable(result):
                event_tasks.append(asyncio.ensure_future(result))

    try:
        launch_backup = False

        if primary_started is not None and not primary_started.is_set():
            started_task = asyncio.create_task(primary_started.wait())
            done, _ = await asyncio.wait(
                {primary_task, started_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if primary_task in done:
                first = _task_result(primary_task)
                if first is _STREAM_EXHAUSTED:
                    launch_backup = True
                    primary_failed = True
                elif isinstance(first, Exception):
                    primary_error = first
                    primary_failed = True
                    launch_backup = True
                elif is_success(first):
                    yield first
                    async for item in primary:
                        yield item
                    return
                else:
                    yield first
                    return

        if not launch_backup:
            done, _ = await asyncio.wait({primary_task}, timeout=delay_seconds)
            if done:
                first = _task_result(primary_task)
                if first is _STREAM_EXHAUSTED:
                    primary_failed = True
                    launch_backup = True
                elif isinstance(first, Exception):
                    primary_error = first
                    primary_failed = True
                    launch_backup = True
                elif is_success(first):
                    yield first
                    async for item in primary:
                        yield item
                    return
                else:
                    yield first
                    return
            else:
                launch_backup = True

        if not launch_backup:
            return

        backup = backup_factory()
        emit("triggered")
        backup_task = asyncio.create_task(_first_decisive_item(backup, ignore))

        pending = {task for task in (primary_task, backup_task) if not task.done()}
        completed = {task for task in (primary_task, backup_task) if task.done()}

        while completed or pending:
            if not completed:
                completed, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )

            # A deterministic tie goes to the primary stream.
            ordered = [task for task in (primary_task, backup_task) if task in completed]
            completed.difference_update(ordered)
            for task in ordered:
                is_primary = task is primary_task
                item = _task_result(task)
                if item is _STREAM_EXHAUSTED:
                    item = None

                if item is not None and not isinstance(item, Exception) and is_success(item):
                    winner = primary if is_primary else backup
                    loser = backup if is_primary else primary
                    loser_task = backup_task if is_primary else primary_task
                    emit("primary_won" if is_primary else "backup_won")
                    if not is_primary and primary_failed:
                        emit("rescued")
                    await _cancel_task(loser_task)
                    await _close_stream(loser)
                    yield item
                    async for next_item in winner:
                        yield next_item
                    return

                if is_primary:
                    primary_failed = True
                    if item is not None:
                        primary_error = item
                elif item is not None:
                    backup_error = item

            if not completed and pending:
                continue
            if not pending:
                break

        error = primary_error if primary_error is not None else backup_error
        if error is not None:
            yield error
    finally:
        await _cancel_task(started_task)
        await _cancel_task(primary_task)
        await _cancel_task(backup_task)
        await _close_stream(primary)
        await _close_stream(backup)
        if event_tasks:
            await asyncio.gather(*event_tasks, return_exceptions=True)
