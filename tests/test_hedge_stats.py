import json
import time
from datetime import datetime, timedelta, timezone

import aiosqlite
import pytest

from src.storage.sqlite_manager import SQLiteManager


def credential(name):
    return {
        "access_token": f"token-{name}",
        "refresh_token": f"refresh-{name}",
        "project_id": f"project-{name}",
        "expiry": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    }


@pytest.mark.asyncio
async def test_sqlite_hedge_stats_count_two_upstream_requests_per_backup(tmp_path, monkeypatch):
    monkeypatch.setenv("CREDENTIALS_DIR", str(tmp_path))
    manager = SQLiteManager()
    await manager.initialize()

    await manager.record_hedge_event("geminicli", "triggered")
    await manager.record_hedge_event("geminicli", "primary_started")
    await manager.record_hedge_event("geminicli", "backup_started")
    await manager.record_hedge_event("geminicli", "backup_won")
    await manager.record_hedge_event("geminicli", "rescued")

    stats = await manager.get_hedge_stats("geminicli")
    assert stats["triggered"] == 1
    assert stats["upstream_requests"] == 2
    assert stats["extra_requests"] == 1
    assert stats["primary_won"] == 0
    assert stats["backup_won"] == 1
    assert stats["rescued"] == 1

    await manager.close()


@pytest.mark.asyncio
async def test_trigger_without_available_backup_does_not_count_extra_request(tmp_path, monkeypatch):
    monkeypatch.setenv("CREDENTIALS_DIR", str(tmp_path))
    manager = SQLiteManager()
    await manager.initialize()

    await manager.record_hedge_event("geminicli", "triggered")
    await manager.record_hedge_event("geminicli", "primary_started")

    stats = await manager.get_hedge_stats("geminicli")
    assert stats["triggered"] == 1
    assert stats["upstream_requests"] == 1
    assert stats["extra_requests"] == 0

    await manager.close()


@pytest.mark.asyncio
async def test_backup_credential_excludes_primary(tmp_path, monkeypatch):
    monkeypatch.setenv("CREDENTIALS_DIR", str(tmp_path))
    manager = SQLiteManager()
    await manager.initialize()
    await manager.store_credential("primary.json", credential("primary"))
    await manager.store_credential("backup.json", credential("backup"))

    result = await manager.get_next_available_credential(
        mode="geminicli",
        model_name="gemini-2.5-flash",
        excluded_filenames=["primary.json"],
    )

    assert result is not None
    assert result[0] == "backup.json"
    await manager.close()


@pytest.mark.asyncio
async def test_old_hedge_table_migrates_without_column_order_corruption(tmp_path, monkeypatch):
    monkeypatch.setenv("CREDENTIALS_DIR", str(tmp_path))
    db_path = tmp_path / "credentials.db"
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE delayed_hedge_stats (
                mode TEXT PRIMARY KEY,
                triggered INTEGER NOT NULL DEFAULT 0,
                extra_requests INTEGER NOT NULL DEFAULT 0,
                primary_won INTEGER NOT NULL DEFAULT 0,
                backup_won INTEGER NOT NULL DEFAULT 0,
                rescued INTEGER NOT NULL DEFAULT 0,
                updated_at REAL
            )
        """)
        await db.execute(
            "INSERT INTO delayed_hedge_stats VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("geminicli", 3, 2, 1, 1, 1, time.time()),
        )
        await db.commit()

    manager = SQLiteManager()
    await manager.initialize()
    stats = await manager.get_hedge_stats("geminicli")

    assert stats["triggered"] == 3
    assert stats["upstream_requests"] == 0
    assert stats["extra_requests"] == 2
    assert stats["primary_won"] == 1
    assert stats["backup_won"] == 1
    assert stats["rescued"] == 1
    await manager.close()
