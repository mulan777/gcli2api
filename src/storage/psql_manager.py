"""
PostgreSQL 存储管理器
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from log import log


def _today_beijing_str() -> str:
    """返回当前京区时间 yyyy-mm-dd。"""
    return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d")


# 模型家族归一化：各种变种（-search / -thinking / -lite / preview / pro / flash 等）
# 会被映射到其基础系列。按“更特殊在前”的顺序匹配。
MODEL_FAMILY_RULES = [
    # 3.5 系（Antigravity 后端别名：低/中/高 thinking budget 的 Gemini 3.5 Flash）
    ("gemini-3.5-flash",              ("3.5-flash",      "3.5-flash")),
    ("gemini-3-flash-agent",          ("3.5-flash",      "3.5-flash-high")),
    # 3.1 系
    ("gemini-3.1-flash-lite-preview", ("3.1-flash-lite", "3.1-flash-lite-preview")),
    ("gemini-3.1-flash-lite",         ("3.1-flash-lite", "3.1-flash-lite")),
    ("gemini-3.1-flash-image",        ("3.1-flash-image","3.1-flash-image")),
    ("gemini-3.1-pro-preview",        ("3.1-pro",        "3.1-pro-preview")),
    ("gemini-3.1-pro",                ("3.1-pro",        "3.1-pro")),
    ("gemini-3.1-flash",              ("3.1-flash",      "3.1-flash")),
    # 3.0 系
    ("gemini-3-flash-preview",        ("3-flash",        "3-flash-preview")),
    ("gemini-3-pro-preview",          ("3-pro",          "3-pro-preview")),
    ("gemini-3-flash",                ("3-flash",        "3-flash")),
    ("gemini-3-pro",                  ("3-pro",          "3-pro")),
    # 2.5 系
    ("gemini-2.5-flash-lite",         ("2.5-flash-lite", "2.5-flash-lite")),
    ("gemini-2.5-flash",              ("2.5-flash",      "2.5-flash")),
    ("gemini-2.5-pro",                ("2.5-pro",        "2.5-pro")),
    # 2.0 / 其他常见家族（预留，避免丢失）
    ("gemini-2.0-flash",              ("2.0-flash",      "2.0-flash")),
    ("gemini-2.0-pro",                ("2.0-pro",        "2.0-pro")),
    # Antigravity 专用别名（无版本号 agent 后缀）
    ("gemini-pro-agent",              ("pro-agent",      "pro-agent")),
    ("claude-opus-4-6",               ("claude-opus-4-6","claude-opus-4-6")),
    ("claude-sonnet-4-6",             ("claude-sonnet-4-6","claude-sonnet-4-6")),
    ("gpt-oss-120b",                  ("gpt-oss-120b",   "gpt-oss-120b")),
]


def normalize_model_family(model_name: Optional[str]) -> str:
    """将模型名归一化为家族 key。

    例如 'gemini-2.5-pro-search' / 'gemini-2.5-pro-thinking' 均归为 '2.5-pro'。
    未识别的返回 'other'。空返回 'unknown'。
    """
    if not model_name:
        return "unknown"
    name = str(model_name).strip().lower()
    # 剧本会传入带前缀 '流式抗截断/' 之类的，去掉
    if "/" in name:
        name = name.rsplit("/", 1)[-1]
    for prefix, (_short, family) in MODEL_FAMILY_RULES:
        if name.startswith(prefix):
            return family
    # 带 antigravity 名字、或未知型号
    return "other"


class PSQLManager:
    """PostgreSQL 数据库管理器"""

    # 状态字段常量
    STATE_FIELDS = {
        "error_codes",
        "error_messages",
        "disabled",
        "permanent_disabled",
        "cycle_stats",
        "last_cycle_stats",
        "last_success",
        "user_email",
        "model_cooldowns",
        "model_disabled",
        "preview",
        "tier",
        "enable_credit",
        "success_count",
        "failure_count",
        "remark",
    }

    def __init__(self):
        self._dsn: Optional[str] = None
        self._pool: Optional[asyncpg.Pool] = None
        self._initialized = False
        self._lock = asyncio.Lock()

        # 内存配置缓存
        self._config_cache: Dict[str, Any] = {}
        self._config_loaded = False

    async def initialize(self) -> None:
        """初始化 PostgreSQL 数据库"""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            try:
                self._dsn = os.getenv("POSTGRESQL_URI", "")
                if not self._dsn:
                    raise RuntimeError("POSTGRESQL_URI environment variable is not set")

                self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)

                async with self._pool.acquire() as conn:
                    await self._create_tables(conn)
                    await self._ensure_schema_compatibility(conn)

                await self._load_config_cache()

                self._initialized = True
                log.info("PostgreSQL storage initialized")

            except Exception as e:
                log.error(f"Error initializing PostgreSQL: {e}")
                if self._pool:
                    await self._pool.close()
                    self._pool = None
                raise

    async def _create_tables(self, conn: asyncpg.Connection) -> None:
        """创建数据库表和索引"""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS credentials (
                id SERIAL PRIMARY KEY,
                filename TEXT UNIQUE NOT NULL,
                credential_data TEXT NOT NULL,

                disabled INTEGER DEFAULT 0,
                permanent_disabled INTEGER DEFAULT 0,
                cycle_stats TEXT DEFAULT '{}',
                last_cycle_stats TEXT DEFAULT '{}',
                error_codes TEXT DEFAULT '[]',
                error_messages TEXT DEFAULT '[]',
                last_success DOUBLE PRECISION,
                user_email TEXT,

                model_cooldowns TEXT DEFAULT '{}',
                preview INTEGER DEFAULT 1,
                tier TEXT DEFAULT 'pro',

                rotation_order INTEGER DEFAULT 0,
                call_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0,
                remark TEXT DEFAULT '',

                created_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW()),
                updated_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS antigravity_credentials (
                id SERIAL PRIMARY KEY,
                filename TEXT UNIQUE NOT NULL,
                credential_data TEXT NOT NULL,

                disabled INTEGER DEFAULT 0,
                permanent_disabled INTEGER DEFAULT 0,
                cycle_stats TEXT DEFAULT '{}',
                last_cycle_stats TEXT DEFAULT '{}',
                error_codes TEXT DEFAULT '[]',
                error_messages TEXT DEFAULT '[]',
                last_success DOUBLE PRECISION,
                user_email TEXT,

                model_cooldowns TEXT DEFAULT '{}',
                model_disabled TEXT DEFAULT '{}',
                tier TEXT DEFAULT 'pro',
                enable_credit INTEGER DEFAULT 0,

                rotation_order INTEGER DEFAULT 0,
                call_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0,
                remark TEXT DEFAULT '',

                created_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW()),
                updated_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())
            )
        """)

        # 按日调用统计（按 京区时间 yyyy-mm-dd 聯合主键）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT NOT NULL,
                mode TEXT NOT NULL,
                success_count BIGINT NOT NULL DEFAULT 0,
                failure_count BIGINT NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW()),
                PRIMARY KEY (date, mode)
            )
        """)

        # 按模型家族的每日统计
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_model_stats (
                date TEXT NOT NULL,
                mode TEXT NOT NULL,
                model_family TEXT NOT NULL,
                success_count BIGINT NOT NULL DEFAULT 0,
                failure_count BIGINT NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW()),
                PRIMARY KEY (date, mode, model_family)
            )
        """)

        # 按分钟中文梅提统计（用于 RPM）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS minute_model_stats (
                minute_ts BIGINT NOT NULL,
                mode TEXT NOT NULL,
                model_family TEXT NOT NULL,
                count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (minute_ts, mode, model_family)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_minute_model_stats_ts ON minute_model_stats(minute_ts)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS delayed_hedge_stats (
                mode TEXT PRIMARY KEY,
                triggered BIGINT NOT NULL DEFAULT 0,
                upstream_requests BIGINT NOT NULL DEFAULT 0,
                extra_requests BIGINT NOT NULL DEFAULT 0,
                primary_won BIGINT NOT NULL DEFAULT 0,
                backup_won BIGINT NOT NULL DEFAULT 0,
                rescued BIGINT NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())
            )
        """)

        await conn.execute("""
            ALTER TABLE delayed_hedge_stats
            ADD COLUMN IF NOT EXISTS upstream_requests BIGINT NOT NULL DEFAULT 0
        """)

        # 索引
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_disabled ON credentials(disabled)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rotation_order ON credentials(rotation_order)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ag_disabled ON antigravity_credentials(disabled)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ag_rotation_order ON antigravity_credentials(rotation_order)
        """)

        log.debug("PostgreSQL tables and indexes created")

    async def _ensure_schema_compatibility(self, conn: asyncpg.Connection) -> None:
        """确保数据库结构兼容，自动修复缺失的列"""
        required_columns = {
            "credentials": [
                ("disabled", "INTEGER DEFAULT 0"),
                ("error_codes", "TEXT DEFAULT '[]'"),
                ("error_messages", "TEXT DEFAULT '[]'"),
                ("last_success", "DOUBLE PRECISION"),
                ("user_email", "TEXT"),
                ("model_cooldowns", "TEXT DEFAULT '{}'"),
                ("preview", "INTEGER DEFAULT 1"),
                ("tier", "TEXT DEFAULT 'pro'"),
                ("rotation_order", "INTEGER DEFAULT 0"),
                ("call_count", "INTEGER DEFAULT 0"),
                ("success_count", "INTEGER DEFAULT 0"),
                ("failure_count", "INTEGER DEFAULT 0"),
                ("permanent_disabled", "INTEGER DEFAULT 0"),
                ("cycle_stats", "TEXT DEFAULT '{}'"),
                ("last_cycle_stats", "TEXT DEFAULT '{}'"),
                ("remark", "TEXT DEFAULT ''"),
                ("created_at", "DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())"),
                ("updated_at", "DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())"),
            ],
            "antigravity_credentials": [
                ("disabled", "INTEGER DEFAULT 0"),
                ("error_codes", "TEXT DEFAULT '[]'"),
                ("error_messages", "TEXT DEFAULT '[]'"),
                ("last_success", "DOUBLE PRECISION"),
                ("user_email", "TEXT"),
                ("model_cooldowns", "TEXT DEFAULT '{}'"),
                ("model_disabled", "TEXT DEFAULT '{}'"),
                ("tier", "TEXT DEFAULT 'pro'"),
                ("enable_credit", "INTEGER DEFAULT 0"),
                ("rotation_order", "INTEGER DEFAULT 0"),
                ("call_count", "INTEGER DEFAULT 0"),
                ("success_count", "INTEGER DEFAULT 0"),
                ("failure_count", "INTEGER DEFAULT 0"),
                ("permanent_disabled", "INTEGER DEFAULT 0"),
                ("cycle_stats", "TEXT DEFAULT '{}'"),
                ("last_cycle_stats", "TEXT DEFAULT '{}'"),
                ("remark", "TEXT DEFAULT ''"),
                ("created_at", "DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())"),
                ("updated_at", "DOUBLE PRECISION DEFAULT EXTRACT(EPOCH FROM NOW())"),
            ],
        }

        try:
            for table_name, columns in required_columns.items():
                rows = await conn.fetch("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = $1
                """, table_name)
                existing = {r["column_name"] for r in rows}

                for col_name, col_def in columns:
                    if col_name not in existing:
                        try:
                            await conn.execute(
                                f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}"
                            )
                            log.info(f"Added missing column {table_name}.{col_name}")
                        except Exception as e:
                            log.error(f"Failed to add column {table_name}.{col_name}: {e}")
        except Exception as e:
            log.error(f"Error ensuring schema compatibility: {e}")

    async def _load_config_cache(self) -> None:
        """加载配置到内存缓存"""
        if self._config_loaded:
            return

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("SELECT key, value FROM config")

            for row in rows:
                try:
                    self._config_cache[row["key"]] = json.loads(row["value"])
                except json.JSONDecodeError:
                    self._config_cache[row["key"]] = row["value"]

            self._config_loaded = True
            log.debug(f"Loaded {len(self._config_cache)} config items into cache")

        except Exception as e:
            log.error(f"Error loading config cache: {e}")
            self._config_cache = {}

    async def close(self) -> None:
        """关闭数据库连接池"""
        if self._pool:
            await self._pool.close()
            self._pool = None
        self._initialized = False
        log.debug("PostgreSQL storage closed")

    async def record_hedge_event(self, mode: str, event: str) -> None:
        """Atomically record delayed hedge usage and outcomes."""
        self._ensure_initialized()
        columns = {
            "triggered": ("triggered",),
            "primary_started": ("upstream_requests",),
            "backup_started": ("upstream_requests", "extra_requests"),
            "primary_won": ("primary_won",),
            "backup_won": ("backup_won",),
            "rescued": ("rescued",),
        }.get(event)
        if not columns:
            return
        updates = ", ".join(f"{column} = delayed_hedge_stats.{column} + 1" for column in columns)
        fields = (
            "triggered", "upstream_requests", "extra_requests",
            "primary_won", "backup_won", "rescued",
        )
        initial = {name: 1 if name in columns else 0 for name in fields}
        async with self._pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO delayed_hedge_stats
                    (mode, triggered, upstream_requests, extra_requests,
                     primary_won, backup_won, rescued, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, EXTRACT(EPOCH FROM NOW()))
                ON CONFLICT (mode) DO UPDATE SET
                    {updates}, updated_at = EXTRACT(EPOCH FROM NOW())
            """, mode, initial["triggered"], initial["upstream_requests"],
                initial["extra_requests"], initial["primary_won"],
                initial["backup_won"], initial["rescued"])

    async def get_hedge_stats(self, mode: Optional[str] = None) -> Dict[str, Any]:
        self._ensure_initialized()
        async with self._pool.acquire() as conn:
            if mode:
                rows = await conn.fetch("SELECT * FROM delayed_hedge_stats WHERE mode = $1", mode)
            else:
                rows = await conn.fetch("SELECT * FROM delayed_hedge_stats")
        fields = (
            "triggered", "upstream_requests", "extra_requests",
            "primary_won", "backup_won", "rescued",
        )
        totals = {field: sum(int(row[field] or 0) for row in rows) for field in fields}
        by_mode = {
            row["mode"]: {field: int(row[field] or 0) for field in fields}
            for row in rows
        }
        return {**totals, "by_mode": by_mode}

    def _ensure_initialized(self) -> None:
        if not self._initialized or not self._pool:
            raise RuntimeError("PostgreSQL manager not initialized")

    def _get_table_name(self, mode: str) -> str:
        if mode == "antigravity":
            return "antigravity_credentials"
        elif mode == "geminicli":
            return "credentials"
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'geminicli' or 'antigravity'")

    # ============ 凭证查询方法 ============

    async def get_next_available_credential(
        self, mode: str = "geminicli", model_name: Optional[str] = None,
        excluded_filenames: Optional[List[str]] = None,
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        """随机获取一个可用凭证（负载均衡）"""
        self._ensure_initialized()

        try:
            table_name = self._get_table_name(mode)
            current_time = time.time()
            excluded = [os.path.basename(name) for name in (excluded_filenames or [])]

            async with self._pool.acquire() as conn:
                if mode == "geminicli":
                    rows = await conn.fetch(f"""
                        SELECT filename, credential_data, model_cooldowns, preview
                        FROM {table_name}
                        WHERE disabled = 0 AND NOT (filename = ANY($1::text[]))
                        ORDER BY RANDOM()
                    """, excluded)

                    if not model_name:
                        if rows:
                            return rows[0]["filename"], json.loads(rows[0]["credential_data"])
                        return None

                    is_preview_model = "preview" in model_name.lower()
                    non_preview_creds = []
                    preview_creds = []

                    for row in rows:
                        model_cooldowns = json.loads(row["model_cooldowns"] or "{}")
                        cd = model_cooldowns.get(model_name)
                        if cd is None or current_time >= cd:
                            if row["preview"]:
                                preview_creds.append((row["filename"], row["credential_data"]))
                            else:
                                non_preview_creds.append((row["filename"], row["credential_data"]))

                    if is_preview_model:
                        if preview_creds:
                            return preview_creds[0][0], json.loads(preview_creds[0][1])
                    else:
                        if non_preview_creds:
                            return non_preview_creds[0][0], json.loads(non_preview_creds[0][1])
                        elif preview_creds:
                            return preview_creds[0][0], json.loads(preview_creds[0][1])

                    return None
                else:
                    rows = await conn.fetch(f"""
                        SELECT filename, credential_data, model_cooldowns, model_disabled, enable_credit
                        FROM {table_name}
                        WHERE disabled = 0 AND NOT (filename = ANY($1::text[]))
                        ORDER BY RANDOM()
                    """, excluded)

                    if not model_name:
                        if rows:
                            credential_data = json.loads(rows[0]["credential_data"])
                            credential_data["enable_credit"] = bool(rows[0]["enable_credit"])
                            return rows[0]["filename"], credential_data
                        return None

                    is_claude_model = "claude" in model_name.lower()
                    for row in rows:
                        model_disabled = json.loads(row["model_disabled"] or "{}")
                        if is_claude_model and model_disabled.get("claude"):
                            continue
                        model_cooldowns = json.loads(row["model_cooldowns"] or "{}")
                        cd = model_cooldowns.get(model_name)
                        if cd is None or current_time >= cd:
                            credential_data = json.loads(row["credential_data"])
                            credential_data["enable_credit"] = bool(row["enable_credit"])
                            credential_data["model_disabled"] = model_disabled
                            return row["filename"], credential_data

                    return None

        except Exception as e:
            log.error(f"Error getting next available credential (mode={mode}, model_name={model_name}): {e}")
            return None

    async def get_available_credentials_list(self) -> List[str]:
        """获取所有可用凭证列表"""
        self._ensure_initialized()

        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT filename FROM credentials
                    WHERE disabled = 0
                    ORDER BY rotation_order ASC
                """)
                return [r["filename"] for r in rows]
        except Exception as e:
            log.error(f"Error getting available credentials list: {e}")
            return []

    # ============ StorageBackend 协议方法 ============

    async def store_credential(self, filename: str, credential_data: Dict[str, Any], mode: str = "geminicli") -> bool:
        """存储或更新凭证"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                existing = await conn.fetchrow(
                    f"SELECT rotation_order FROM {table_name} WHERE filename = $1", filename
                )

                if existing:
                    await conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET credential_data = $1,
                            updated_at = EXTRACT(EPOCH FROM NOW())
                        WHERE filename = $2
                        """,
                        json.dumps(credential_data), filename
                    )
                else:
                    row = await conn.fetchrow(
                        f"SELECT COALESCE(MAX(rotation_order), -1) + 1 AS next_order FROM {table_name}"
                    )
                    next_order = row["next_order"]
                    await conn.execute(
                        f"""
                        INSERT INTO {table_name}
                        (filename, credential_data, rotation_order, last_success)
                        VALUES ($1, $2, $3, $4)
                        """,
                        filename, json.dumps(credential_data), next_order, time.time()
                    )

            log.debug(f"Stored credential: {filename} (mode={mode})")
            return True

        except Exception as e:
            log.error(f"Error storing credential {filename}: {e}")
            return False

    async def get_credential(self, filename: str, mode: str = "geminicli") -> Optional[Dict[str, Any]]:
        """获取凭证数据"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT credential_data FROM {table_name} WHERE filename = $1", filename
                )
                if row:
                    return json.loads(row["credential_data"])
                return None
        except Exception as e:
            log.error(f"Error getting credential {filename}: {e}")
            return None

    async def list_credentials(self, mode: str = "geminicli") -> List[str]:
        """列出所有凭证文件名（包括禁用的）"""
        self._ensure_initialized()

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    f"SELECT filename FROM {table_name} ORDER BY rotation_order"
                )
                return [r["filename"] for r in rows]
        except Exception as e:
            log.error(f"Error listing credentials: {e}")
            return []

    async def delete_credential(self, filename: str, mode: str = "geminicli") -> bool:
        """删除凭证"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    f"DELETE FROM {table_name} WHERE filename = $1", filename
                )
                # asyncpg returns "DELETE N"
                deleted_count = int(result.split()[-1])

            if deleted_count > 0:
                log.debug(f"Deleted credential: {filename} (mode={mode})")
                return True
            else:
                log.warning(f"No credential found to delete: {filename} (mode={mode})")
                return False

        except Exception as e:
            log.error(f"Error deleting credential {filename}: {e}")
            return False

    async def update_credential_state(self, filename: str, state_updates: Dict[str, Any], mode: str = "geminicli") -> bool:
        """更新凭证状态"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            log.debug(f"[DB] update_credential_state: filename={filename}, updates={state_updates}, mode={mode}")

            set_clauses = []
            values = []
            idx = 1

            for key, value in state_updates.items():
                if key in self.STATE_FIELDS:
                    if key == "enable_credit" and mode != "antigravity":
                        continue
                    if key in ("error_codes", "error_messages", "model_cooldowns", "model_disabled"):
                        set_clauses.append(f"{key} = ${idx}")
                        values.append(json.dumps(value))
                    else:
                        set_clauses.append(f"{key} = ${idx}")
                        values.append(value)
                    idx += 1

            if not set_clauses:
                return True

            set_clauses.append(f"updated_at = EXTRACT(EPOCH FROM NOW())")
            values.append(filename)

            sql = f"""
                UPDATE {table_name}
                SET {', '.join(set_clauses)}
                WHERE filename = ${idx}
            """

            async with self._pool.acquire() as conn:
                result = await conn.execute(sql, *values)
                updated_count = int(result.split()[-1])

            return updated_count > 0

        except Exception as e:
            log.error(f"[DB] Error updating credential state {filename}: {e}")
            return False

    async def get_credential_state(self, filename: str, mode: str = "geminicli") -> Dict[str, Any]:
        """获取凭证状态"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                if mode == "geminicli":
                    row = await conn.fetchrow(f"""
                        SELECT disabled, error_codes, last_success, user_email, model_cooldowns,
                               preview, tier, success_count, failure_count, permanent_disabled, cycle_stats, last_cycle_stats, remark
                        FROM {table_name} WHERE filename = $1
                    """, filename)

                    if row:
                        return {
                            "disabled": bool(row["disabled"]),
                            "error_codes": json.loads(row["error_codes"] or "[]"),
                            "last_success": row["last_success"] or time.time(),
                            "user_email": row["user_email"],
                            "model_cooldowns": json.loads(row["model_cooldowns"] or "{}"),
                            "preview": bool(row["preview"]) if row["preview"] is not None else True,
                            "tier": row["tier"] if row["tier"] is not None else "pro",
                            "success_count": row["success_count"] or 0,
                            "failure_count": row["failure_count"] or 0,
                            "permanent_disabled": bool(row["permanent_disabled"]),
                            "cycle_stats": json.loads(row["cycle_stats"] or "{}"),
                            "last_cycle_stats": json.loads(row["last_cycle_stats"] or "{}"),
                            "remark": row["remark"] or "",
                        }

                    return {
                        "disabled": False,
                        "error_codes": [],
                        "last_success": time.time(),
                        "user_email": None,
                        "model_cooldowns": {},
                        "preview": True,
                        "tier": "pro",
                        "success_count": 0,
                        "failure_count": 0,
                        "remark": "",
                    }
                else:
                    row = await conn.fetchrow(f"""
                        SELECT disabled, error_codes, last_success, user_email, model_cooldowns, model_disabled,
                               tier, enable_credit, success_count, failure_count, permanent_disabled, cycle_stats, last_cycle_stats, remark
                        FROM {table_name} WHERE filename = $1
                    """, filename)

                    if row:
                        return {
                            "disabled": bool(row["disabled"]),
                            "error_codes": json.loads(row["error_codes"] or "[]"),
                            "last_success": row["last_success"] or time.time(),
                            "user_email": row["user_email"],
                            "model_cooldowns": json.loads(row["model_cooldowns"] or "{}"),
                            "model_disabled": json.loads(row["model_disabled"] or "{}"),
                            "tier": row["tier"] if row["tier"] is not None else "pro",
                            "enable_credit": bool(row["enable_credit"]) if row["enable_credit"] is not None else False,
                            "success_count": row["success_count"] or 0,
                            "failure_count": row["failure_count"] or 0,
                            "permanent_disabled": bool(row["permanent_disabled"]),
                            "cycle_stats": json.loads(row["cycle_stats"] or "{}"),
                            "last_cycle_stats": json.loads(row["last_cycle_stats"] or "{}"),
                            "remark": row["remark"] or "",
                        }

                    return {
                        "disabled": False,
                        "error_codes": [],
                        "last_success": time.time(),
                        "user_email": None,
                        "model_cooldowns": {},
                        "model_disabled": {},
                        "tier": "pro",
                        "enable_credit": False,
                        "success_count": 0,
                        "failure_count": 0,
                        "remark": "",
                    }

        except Exception as e:
            log.error(f"Error getting credential state {filename}: {e}")
            return {}

    async def get_all_credential_states(self, mode: str = "geminicli") -> Dict[str, Dict[str, Any]]:
        """获取所有凭证状态"""
        self._ensure_initialized()

        try:
            table_name = self._get_table_name(mode)
            current_time = time.time()

            async with self._pool.acquire() as conn:
                if mode == "geminicli":
                    rows = await conn.fetch(f"""
                        SELECT filename, disabled, error_codes, last_success,
                               user_email, model_cooldowns, preview, tier,
                               success_count, failure_count, permanent_disabled, cycle_stats, last_cycle_stats, remark
                        FROM {table_name}
                    """)

                    states = {}
                    for row in rows:
                        model_cooldowns = json.loads(row["model_cooldowns"] or "{}")
                        if model_cooldowns:
                            model_cooldowns = {k: v for k, v in model_cooldowns.items() if v > current_time}

                        states[row["filename"]] = {
                            "disabled": bool(row["disabled"]),
                            "error_codes": json.loads(row["error_codes"] or "[]"),
                            "last_success": row["last_success"] or current_time,
                            "user_email": row["user_email"],
                            "model_cooldowns": model_cooldowns,
                            "preview": bool(row["preview"]) if row["preview"] is not None else True,
                            "tier": row["tier"] if row["tier"] is not None else "pro",
                            "success_count": row["success_count"] or 0,
                            "failure_count": row["failure_count"] or 0,
                            "permanent_disabled": bool(row["permanent_disabled"]),
                            "cycle_stats": json.loads(row["cycle_stats"] or "{}"),
                            "last_cycle_stats": json.loads(row["last_cycle_stats"] or "{}"),
                            "remark": row["remark"] or "",
                        }
                    return states
                else:
                    rows = await conn.fetch(f"""
                        SELECT filename, disabled, error_codes, last_success,
                               user_email, model_cooldowns, model_disabled, tier, enable_credit,
                               success_count, failure_count, permanent_disabled, cycle_stats, last_cycle_stats, remark
                        FROM {table_name}
                    """)

                    states = {}
                    for row in rows:
                        model_cooldowns = json.loads(row["model_cooldowns"] or "{}")
                        if model_cooldowns:
                            model_cooldowns = {k: v for k, v in model_cooldowns.items() if v > current_time}

                        states[row["filename"]] = {
                            "disabled": bool(row["disabled"]),
                            "error_codes": json.loads(row["error_codes"] or "[]"),
                            "last_success": row["last_success"] or current_time,
                            "user_email": row["user_email"],
                            "model_cooldowns": model_cooldowns,
                            "model_disabled": json.loads(row["model_disabled"] or "{}"),
                            "tier": row["tier"] if row["tier"] is not None else "pro",
                            "enable_credit": bool(row["enable_credit"]) if row["enable_credit"] is not None else False,
                            "success_count": row["success_count"] or 0,
                            "failure_count": row["failure_count"] or 0,
                            "permanent_disabled": bool(row["permanent_disabled"]),
                            "cycle_stats": json.loads(row["cycle_stats"] or "{}"),
                            "last_cycle_stats": json.loads(row["last_cycle_stats"] or "{}"),
                            "remark": row["remark"] or "",
                        }
                    return states

        except Exception as e:
            log.error(f"Error getting all credential states: {e}")
            return {}

    async def get_credentials_summary(
        self,
        offset: int = 0,
        limit: Optional[int] = None,
        status_filter: str = "all",
        mode: str = "geminicli",
        error_code_filter: Optional[str] = None,
        cooldown_filter: Optional[str] = None,
        preview_filter: Optional[str] = None,
        tier_filter: Optional[str] = None,
        remark_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取凭证的摘要信息，支持分页和状态筛选"""
        self._ensure_initialized()

        try:
            table_name = self._get_table_name(mode)
            current_time = time.time()

            async with self._pool.acquire() as conn:
                # 全局统计
                stats_rows = await conn.fetch(
                    f"SELECT disabled, permanent_disabled, COUNT(*) AS cnt FROM {table_name} GROUP BY disabled, permanent_disabled"
                )
                global_stats = {"total": 0, "normal": 0, "disabled": 0, "permanent_disabled": 0}
                for r in stats_rows:
                    global_stats["total"] += r["cnt"]
                    if r["permanent_disabled"]:
                        global_stats["permanent_disabled"] += r["cnt"]
                    elif r["disabled"]:
                        global_stats["disabled"] += r["cnt"]
                    else:
                        global_stats["normal"] += r["cnt"]

                # WHERE 子句
                where_clauses = []
                if status_filter == "enabled":
                    where_clauses.append("disabled = 0 AND COALESCE(permanent_disabled, 0) = 0")
                elif status_filter == "disabled":
                    where_clauses.append("disabled = 1 AND COALESCE(permanent_disabled, 0) = 0")
                elif status_filter == "permanent_disabled":
                    where_clauses.append("COALESCE(permanent_disabled, 0) = 1")

                where_clause = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

                # 查询
                if mode == "geminicli":
                    all_rows = await conn.fetch(f"""
                        SELECT filename, disabled, error_codes, last_success,
                               user_email, rotation_order, model_cooldowns, preview, tier,
                               success_count, failure_count, permanent_disabled, cycle_stats, last_cycle_stats, remark
                        FROM {table_name}
                        {where_clause}
                        ORDER BY rotation_order
                    """)
                else:
                    all_rows = await conn.fetch(f"""
                        SELECT filename, disabled, error_codes, last_success,
                               user_email, rotation_order, model_cooldowns, model_disabled, tier, enable_credit,
                               success_count, failure_count, permanent_disabled, cycle_stats, last_cycle_stats, remark
                        FROM {table_name}
                        {where_clause}
                        ORDER BY rotation_order
                    """)

                # 错误码筛选
                filter_value = None
                filter_int = None
                filter_none = False
                if error_code_filter and str(error_code_filter).strip().lower() != "all":
                    if str(error_code_filter).strip().lower() == "none":
                        filter_none = True
                    else:
                        filter_value = str(error_code_filter).strip()
                        try:
                            filter_int = int(filter_value)
                        except ValueError:
                            filter_int = None

                all_summaries = []
                for row in all_rows:
                    error_codes_json = row["error_codes"] or "[]"
                    model_cooldowns = json.loads(row["model_cooldowns"] or "{}")
                    active_cooldowns = {k: v for k, v in model_cooldowns.items() if v > current_time}
                    error_codes = json.loads(error_codes_json)

                    # 筛选无错误的凭证
                    if filter_none:
                        if error_codes:
                            continue

                    if filter_value:
                        match = False
                        for code in error_codes:
                            if code == filter_value or code == filter_int:
                                match = True
                                break
                            if isinstance(code, str) and filter_int is not None:
                                try:
                                    if int(code) == filter_int:
                                        match = True
                                        break
                                except ValueError:
                                    pass
                        if not match:
                            continue

                    row_remark = row["remark"] or ""
                    if remark_filter is not None and row_remark != remark_filter:
                        continue

                    summary = {
                        "filename": row["filename"],
                        "disabled": bool(row["disabled"]),
                        "permanent_disabled": bool(row["permanent_disabled"]),
                        "error_codes": error_codes,
                        "last_success": row["last_success"] or current_time,
                        "user_email": row["user_email"],
                        "rotation_order": row["rotation_order"],
                        "model_cooldowns": active_cooldowns,
                        "model_disabled": json.loads(row["model_disabled"] or "{}") if mode == "antigravity" else {},
                        "tier": row["tier"] if row["tier"] is not None else "pro",
                        "success_count": row["success_count"] or 0,
                        "failure_count": row["failure_count"] or 0,
                        "cycle_stats": json.loads(row["cycle_stats"] or "{}"),
                        "last_cycle_stats": json.loads(row["last_cycle_stats"] or "{}"),
                        "remark": row_remark,
                    }

                    if mode == "geminicli":
                        summary["preview"] = bool(row["preview"]) if row["preview"] is not None else True

                        if preview_filter:
                            preview_value = summary.get("preview", True)
                            if preview_filter == "preview" and not preview_value:
                                continue
                            elif preview_filter == "no_preview" and preview_value:
                                continue
                    else:
                        summary["enable_credit"] = bool(row["enable_credit"]) if row["enable_credit"] is not None else False

                    if tier_filter and tier_filter in ("free", "pro", "ultra"):
                        if summary["tier"] != tier_filter:
                            continue

                    if cooldown_filter == "in_cooldown":
                        if active_cooldowns:
                            all_summaries.append(summary)
                    elif cooldown_filter == "no_cooldown":
                        if not active_cooldowns:
                            all_summaries.append(summary)
                    elif cooldown_filter == "pro_no_cooldown":
                        # 只保留 Pro 系列未冷却的凭证（不管 Flash 是否冷却）
                        if not any("pro" in k.lower() for k in active_cooldowns):
                            all_summaries.append(summary)
                    elif cooldown_filter == "flash_no_cooldown":
                        # 只保留 Flash 系列未冷却的凭证（不管 Pro 是否冷却）
                        if not any("flash" in k.lower() for k in active_cooldowns):
                            all_summaries.append(summary)
                    elif cooldown_filter == "claude_no_cooldown":
                        # 只保留 Claude 系列未冷却的凭证（不管 Pro/Flash 是否冷却）
                        if not any("claude" in k.lower() for k in active_cooldowns):
                            all_summaries.append(summary)
                    else:
                        all_summaries.append(summary)

                total_count = len(all_summaries)
                if limit is not None:
                    summaries = all_summaries[offset:offset + limit]
                else:
                    summaries = all_summaries[offset:]

                return {
                    "items": summaries,
                    "total": total_count,
                    "offset": offset,
                    "limit": limit,
                    "stats": global_stats,
                }

        except Exception as e:
            log.error(f"Error getting credentials summary: {e}")
            return {
                "items": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "stats": {"total": 0, "normal": 0, "disabled": 0, "permanent_disabled": 0},
            }

    async def get_duplicate_credentials_by_email(self, mode: str = "geminicli") -> Dict[str, Any]:
        """获取按邮箱分组的重复凭证信息"""
        self._ensure_initialized()

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    f"SELECT filename, user_email FROM {table_name} ORDER BY filename"
                )

            email_to_files: Dict[str, List[str]] = {}
            no_email_files: List[str] = []

            for row in rows:
                if row["user_email"]:
                    email_to_files.setdefault(row["user_email"], []).append(row["filename"])
                else:
                    no_email_files.append(row["filename"])

            duplicate_groups = []
            total_duplicate_count = 0
            for email, files in email_to_files.items():
                if len(files) > 1:
                    duplicate_groups.append({
                        "email": email,
                        "kept_file": files[0],
                        "duplicate_files": files[1:],
                        "duplicate_count": len(files) - 1,
                    })
                    total_duplicate_count += len(files) - 1

            return {
                "email_groups": email_to_files,
                "duplicate_groups": duplicate_groups,
                "duplicate_count": total_duplicate_count,
                "no_email_files": no_email_files,
                "no_email_count": len(no_email_files),
                "unique_email_count": len(email_to_files),
                "total_count": len(rows),
            }

        except Exception as e:
            log.error(f"Error getting duplicate credentials by email: {e}")
            return {
                "email_groups": {},
                "duplicate_groups": [],
                "duplicate_count": 0,
                "no_email_files": [],
                "no_email_count": 0,
                "unique_email_count": 0,
                "total_count": 0,
            }

    # ============ 配置管理（内存缓存）============

    async def set_config(self, key: str, value: Any) -> bool:
        """设置配置（写入数据库 + 更新内存缓存）"""
        self._ensure_initialized()

        try:
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO config (key, value, updated_at)
                    VALUES ($1, $2, EXTRACT(EPOCH FROM NOW()))
                    ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value,
                            updated_at = EXCLUDED.updated_at
                """, key, json.dumps(value))

            self._config_cache[key] = value
            return True

        except Exception as e:
            log.error(f"Error setting config {key}: {e}")
            return False

    async def reload_config_cache(self) -> None:
        """重新加载配置缓存"""
        self._ensure_initialized()
        self._config_loaded = False
        await self._load_config_cache()
        log.info("Config cache reloaded from database")

    async def get_config(self, key: str, default: Any = None) -> Any:
        """获取配置（从内存缓存）"""
        self._ensure_initialized()
        return self._config_cache.get(key, default)

    async def get_all_config(self) -> Dict[str, Any]:
        """获取所有配置（从内存缓存）"""
        self._ensure_initialized()
        return self._config_cache.copy()

    async def delete_config(self, key: str) -> bool:
        """删除配置"""
        self._ensure_initialized()

        try:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM config WHERE key = $1", key)

            self._config_cache.pop(key, None)
            return True

        except Exception as e:
            log.error(f"Error deleting config {key}: {e}")
            return False

    async def get_credential_errors(self, filename: str, mode: str = "geminicli") -> Dict[str, Any]:
        """获取凭证的错误信息"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT error_codes, error_messages FROM {table_name} WHERE filename = $1",
                    filename
                )

            if row:
                return {
                    "filename": filename,
                    "error_codes": json.loads(row["error_codes"] or "[]"),
                    "error_messages": json.loads(row["error_messages"] or "[]"),
                }

            return {"filename": filename, "error_codes": [], "error_messages": []}

        except Exception as e:
            log.error(f"Error getting credential errors {filename}: {e}")
            return {"filename": filename, "error_codes": [], "error_messages": [], "error": str(e)}

    # ============ 模型级冷却管理 ============

    async def set_model_cooldown(
        self,
        filename: str,
        model_name: str,
        cooldown_until: Optional[float],
        mode: str = "geminicli"
    ) -> bool:
        """设置特定模型的冷却时间；新增有效冷却时结算上一轮循环统计。"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT model_cooldowns, cycle_stats FROM {table_name} WHERE filename = $1", filename
                )

                if not row:
                    log.warning(f"Credential {filename} not found")
                    return False

                model_cooldowns = json.loads(row["model_cooldowns"] or "{}")
                close_cycle = False
                if cooldown_until is None:
                    model_cooldowns.pop(model_name, None)
                else:
                    previous_until = model_cooldowns.get(model_name)
                    model_cooldowns[model_name] = cooldown_until
                    close_cycle = not previous_until or previous_until <= time.time()

                if close_cycle:
                    new_cycle_stats, last_cycle_stats = self._close_cycle_stats(row["cycle_stats"], model_name)
                    await conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET model_cooldowns = $1,
                            cycle_stats = $2,
                            last_cycle_stats = $3,
                            updated_at = EXTRACT(EPOCH FROM NOW())
                        WHERE filename = $4
                        """,
                        json.dumps(model_cooldowns), new_cycle_stats, last_cycle_stats, filename
                    )
                else:
                    await conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET model_cooldowns = $1,
                            updated_at = EXTRACT(EPOCH FROM NOW())
                        WHERE filename = $2
                        """,
                        json.dumps(model_cooldowns), filename
                    )

            log.debug(f"Set model cooldown: {filename}, model_name={model_name}, cooldown_until={cooldown_until}")
            return True

        except Exception as e:
            log.error(f"Error setting model cooldown for {filename}: {e}")
            return False

    async def clear_all_model_cooldowns(
        self,
        filename: str,
        mode: str = "geminicli"
    ) -> bool:
        """清除某个凭证的所有模型冷却时间"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    f"""
                    UPDATE {table_name}
                    SET model_cooldowns = '{{}}',
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    WHERE filename = $1
                    """,
                    filename,
                )
                updated_count = int(result.split()[-1])

            if updated_count == 0:
                log.warning(f"Credential {filename} not found")
                return False

            log.debug(f"Cleared all model cooldowns: {filename} (mode={mode})")
            return True

        except Exception as e:
            log.error(f"Error clearing all model cooldowns for {filename}: {e}")
            return False

    async def record_success(
        self,
        filename: str,
        model_name: Optional[str] = None,
        mode: str = "geminicli"
    ) -> None:
        """成功调用后的统计写入"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            async with self._pool.acquire() as conn:
                stats_row = await conn.fetchrow(f"SELECT cycle_stats FROM {table_name} WHERE filename = $1", filename)
                await conn.execute(f"""
                    UPDATE {table_name}
                    SET success_count = COALESCE(success_count, 0) + 1,
                        call_count = COALESCE(call_count, 0) + 1,
                        cycle_stats = $2,
                        last_success = EXTRACT(EPOCH FROM NOW()),
                        error_codes = CASE
                            WHEN error_codes IS NOT NULL AND error_codes != '[]' AND error_codes != ''
                            THEN '[]' ELSE error_codes END,
                        error_messages = CASE
                            WHEN error_codes IS NOT NULL AND error_codes != '[]' AND error_codes != ''
                            THEN '{{}}' ELSE error_messages END,
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    WHERE filename = $1
                """, filename, self._bump_cycle_stats(stats_row["cycle_stats"] if stats_row else None, model_name, success=True))

                if model_name:
                    row = await conn.fetchrow(
                        f"SELECT model_cooldowns FROM {table_name} WHERE filename = $1", filename
                    )
                    if row:
                        cooldowns = json.loads(row["model_cooldowns"] or "{}")
                        if model_name in cooldowns:
                            cooldowns.pop(model_name)
                            await conn.execute(
                                f"""
                                UPDATE {table_name}
                                SET model_cooldowns = $1, updated_at = EXTRACT(EPOCH FROM NOW())
                                WHERE filename = $2
                                """,
                                json.dumps(cooldowns), filename
                            )

                # 全局每日统计
                await conn.execute(
                    """
                    INSERT INTO daily_stats (date, mode, success_count, failure_count, updated_at)
                    VALUES ($1, $2, 1, 0, EXTRACT(EPOCH FROM NOW()))
                    ON CONFLICT (date, mode) DO UPDATE
                    SET success_count = daily_stats.success_count + 1,
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    """,
                    _today_beijing_str(), mode,
                )

                # 按模型家族每日 + 分钟调用统计
                family = normalize_model_family(model_name)
                today = _today_beijing_str()
                await conn.execute(
                    """
                    INSERT INTO daily_model_stats (date, mode, model_family, success_count, failure_count, updated_at)
                    VALUES ($1, $2, $3, 1, 0, EXTRACT(EPOCH FROM NOW()))
                    ON CONFLICT (date, mode, model_family) DO UPDATE
                    SET success_count = daily_model_stats.success_count + 1,
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    """,
                    today, mode, family,
                )
                minute_ts = int(time.time() // 60) * 60
                await conn.execute(
                    """
                    INSERT INTO minute_model_stats (minute_ts, mode, model_family, count)
                    VALUES ($1, $2, $3, 1)
                    ON CONFLICT (minute_ts, mode, model_family) DO UPDATE
                    SET count = minute_model_stats.count + 1
                    """,
                    minute_ts, mode, family,
                )

        except Exception as e:
            log.error(f"Error recording success for {filename}: {e}")

    async def record_failure(
        self,
        filename: str,
        error_code: int,
        error_message: Optional[str] = None,
        mode: str = "geminicli",
        model_name: Optional[str] = None,
    ) -> None:
        """记录一次失败调用，并保存最新错误信息。"""
        self._ensure_initialized()
        filename = os.path.basename(filename)

        try:
            table_name = self._get_table_name(mode)
            error_messages = {}
            if error_message:
                error_messages[str(error_code)] = error_message

            async with self._pool.acquire() as conn:
                stats_row = await conn.fetchrow(f"SELECT cycle_stats FROM {table_name} WHERE filename = $1", filename)
                await conn.execute(
                    f"""
                    UPDATE {table_name}
                    SET failure_count = COALESCE(failure_count, 0) + 1,
                        call_count = COALESCE(call_count, 0) + 1,
                        cycle_stats = $1,
                        error_codes = $2,
                        error_messages = $3,
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    WHERE filename = $4
                    """,
                    self._bump_cycle_stats(stats_row["cycle_stats"] if stats_row else None, model_name, success=False),
                    json.dumps([error_code]),
                    json.dumps(error_messages),
                    filename,
                )

                # 全局每日统计
                await conn.execute(
                    """
                    INSERT INTO daily_stats (date, mode, success_count, failure_count, updated_at)
                    VALUES ($1, $2, 0, 1, EXTRACT(EPOCH FROM NOW()))
                    ON CONFLICT (date, mode) DO UPDATE
                    SET failure_count = daily_stats.failure_count + 1,
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    """,
                    _today_beijing_str(), mode,
                )

                # 按模型家族每日 + 分钟统计
                family = normalize_model_family(model_name)
                today = _today_beijing_str()
                await conn.execute(
                    """
                    INSERT INTO daily_model_stats (date, mode, model_family, success_count, failure_count, updated_at)
                    VALUES ($1, $2, $3, 0, 1, EXTRACT(EPOCH FROM NOW()))
                    ON CONFLICT (date, mode, model_family) DO UPDATE
                    SET failure_count = daily_model_stats.failure_count + 1,
                        updated_at = EXTRACT(EPOCH FROM NOW())
                    """,
                    today, mode, family,
                )
                minute_ts = int(time.time() // 60) * 60
                await conn.execute(
                    """
                    INSERT INTO minute_model_stats (minute_ts, mode, model_family, count)
                    VALUES ($1, $2, $3, 1)
                    ON CONFLICT (minute_ts, mode, model_family) DO UPDATE
                    SET count = minute_model_stats.count + 1
                    """,
                    minute_ts, mode, family,
                )

        except Exception as e:
            log.error(f"Error recording failure for {filename}: {e}")


    @staticmethod
    def _model_cycle_family(model_name: Optional[str]) -> str:
        model = (model_name or "").lower()
        if "pro" in model:
            return "pro"
        if "flash" in model:
            return "flash"
        return "other"

    @staticmethod
    def _is_claude_model(model_name: Optional[str]) -> bool:
        return "claude" in (model_name or "").lower()

    @staticmethod
    def _bump_cycle_stats(raw: Optional[str], model_name: Optional[str], success: bool = True) -> str:
        now = time.time()
        try:
            stats = json.loads(raw or "{}")
        except Exception:
            stats = {}
        if not isinstance(stats, dict):
            stats = {}
        stats.setdefault("started_at", now)
        stats.setdefault("pro", 0)
        stats.setdefault("flash", 0)
        stats.setdefault("other", 0)
        stats.setdefault("claude_success", 0)
        stats.setdefault("claude_failure", 0)
        stats["total"] = int(stats.get("total") or 0) + 1
        family = PSQLManager._model_cycle_family(model_name)
        stats[family] = int(stats.get(family) or 0) + 1
        if PSQLManager._is_claude_model(model_name):
            key = "claude_success" if success else "claude_failure"
            stats[key] = int(stats.get(key) or 0) + 1
        stats["updated_at"] = now
        return json.dumps(stats)

    @staticmethod
    def _close_cycle_stats(raw: Optional[str], model_name: Optional[str]) -> tuple[str, str]:
        """Close only the model family that just entered cooldown.

        Pro and Flash quotas are independent buckets. When a Pro model enters
        cooldown, Flash may still be usable, so the current Flash counter should
        keep accumulating instead of being reset together with Pro. The returned
        last_cycle_stats therefore contains the closed family only, while
        cycle_stats keeps the other families as the new current cycle.
        """
        now = time.time()
        try:
            current_stats = json.loads(raw or "{}")
        except Exception:
            current_stats = {}
        if not isinstance(current_stats, dict):
            current_stats = {}

        family = PSQLManager._model_cycle_family(model_name)
        families = ("pro", "flash", "other")

        for key in families:
            current_stats[key] = int(current_stats.get(key) or 0)
        current_stats["total"] = sum(current_stats[key] for key in families)

        last_stats = {
            "started_at": current_stats.get("started_at", now),
            "total": current_stats.get(family, 0),
            "pro": current_stats["pro"] if family == "pro" else 0,
            "flash": current_stats["flash"] if family == "flash" else 0,
            "other": current_stats["other"] if family == "other" else 0,
            "updated_at": current_stats.get("updated_at", now),
            "ended_at": now,
            "cooldown_family": family,
        }

        new_stats = {
            "started_at": now,
            "pro": current_stats["pro"],
            "flash": current_stats["flash"],
            "other": current_stats["other"],
        }
        new_stats[family] = 0
        new_stats["total"] = sum(new_stats[key] for key in families)

        return json.dumps(new_stats), json.dumps(last_stats)

    async def get_today_stats(self, mode: Optional[str] = None) -> Dict[str, Any]:
        """获取今天（北京时间）的总调用统计。

        Args:
            mode: 'geminicli' / 'antigravity' / None(全部)
        """
        self._ensure_initialized()
        today = _today_beijing_str()
        try:
            async with self._pool.acquire() as conn:
                if mode:
                    row = await conn.fetchrow(
                        """
                        SELECT
                            COALESCE(success_count, 0) AS s,
                            COALESCE(failure_count, 0) AS f
                        FROM daily_stats
                        WHERE date = $1 AND mode = $2
                        """,
                        today, mode,
                    )
                    s = int(row["s"]) if row else 0
                    f = int(row["f"]) if row else 0
                    return {
                        "date": today,
                        "mode": mode,
                        "success_count": s,
                        "failure_count": f,
                        "total_count": s + f,
                    }
                else:
                    rows = await conn.fetch(
                        """
                        SELECT mode,
                               COALESCE(success_count, 0) AS s,
                               COALESCE(failure_count, 0) AS f
                        FROM daily_stats
                        WHERE date = $1
                        """,
                        today,
                    )
                    by_mode: Dict[str, Dict[str, int]] = {}
                    total_s = total_f = 0
                    for r in rows:
                        m = r["mode"]
                        s = int(r["s"]); f = int(r["f"])
                        by_mode[m] = {
                            "success_count": s,
                            "failure_count": f,
                            "total_count": s + f,
                        }
                        total_s += s; total_f += f
                    return {
                        "date": today,
                        "by_mode": by_mode,
                        "success_count": total_s,
                        "failure_count": total_f,
                        "total_count": total_s + total_f,
                    }
        except Exception as e:
            log.error(f"get_today_stats failed: {e}")
            return {
                "date": today,
                "success_count": 0,
                "failure_count": 0,
                "total_count": 0,
                "error": str(e),
            }

    async def get_recent_daily_stats(self, days: int = 7, mode: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取最近 N 天的每日调用统计（按北京日期）。"""
        self._ensure_initialized()
        days = max(1, min(int(days or 7), 90))
        try:
            async with self._pool.acquire() as conn:
                if mode:
                    rows = await conn.fetch(
                        """
                        SELECT date,
                               COALESCE(success_count, 0) AS s,
                               COALESCE(failure_count, 0) AS f
                        FROM daily_stats
                        WHERE mode = $1
                        ORDER BY date DESC
                        LIMIT $2
                        """,
                        mode, days,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT date,
                               SUM(success_count) AS s,
                               SUM(failure_count) AS f
                        FROM daily_stats
                        GROUP BY date
                        ORDER BY date DESC
                        LIMIT $1
                        """,
                        days,
                    )
                return [
                    {
                        "date": r["date"],
                        "success_count": int(r["s"] or 0),
                        "failure_count": int(r["f"] or 0),
                        "total_count": int((r["s"] or 0) + (r["f"] or 0)),
                    }
                    for r in rows
                ]
        except Exception as e:
            log.error(f"get_recent_daily_stats failed: {e}")
            return []


    @staticmethod
    def _model_cycle_family(model_name: Optional[str]) -> str:
        model = (model_name or "").lower()
        if "pro" in model:
            return "pro"
        if "flash" in model:
            return "flash"
        return "other"

    @staticmethod
    def _bump_cycle_stats(raw: Optional[str], model_name: Optional[str], success: bool = True) -> str:
        now = time.time()
        try:
            stats = json.loads(raw or "{}")
        except Exception:
            stats = {}
        if not isinstance(stats, dict):
            stats = {}
        stats.setdefault("started_at", now)
        stats.setdefault("pro", 0)
        stats.setdefault("flash", 0)
        stats.setdefault("other", 0)
        stats.setdefault("claude_success", 0)
        stats.setdefault("claude_failure", 0)
        stats["total"] = int(stats.get("total") or 0) + 1
        family = PSQLManager._model_cycle_family(model_name)
        stats[family] = int(stats.get(family) or 0) + 1
        if PSQLManager._is_claude_model(model_name):
            key = "claude_success" if success else "claude_failure"
            stats[key] = int(stats.get(key) or 0) + 1
        stats["updated_at"] = now
        return json.dumps(stats)

    @staticmethod
    def _close_cycle_stats(raw: Optional[str], model_name: Optional[str]) -> tuple[str, str]:
        """Close only the model family that just entered cooldown.

        Pro and Flash quotas are independent buckets. When a Pro model enters
        cooldown, Flash may still be usable, so the current Flash counter should
        keep accumulating instead of being reset together with Pro. The returned
        last_cycle_stats therefore contains the closed family only, while
        cycle_stats keeps the other families as the new current cycle.
        """
        now = time.time()
        try:
            current_stats = json.loads(raw or "{}")
        except Exception:
            current_stats = {}
        if not isinstance(current_stats, dict):
            current_stats = {}

        family = PSQLManager._model_cycle_family(model_name)
        families = ("pro", "flash", "other")

        for key in families:
            current_stats[key] = int(current_stats.get(key) or 0)
        current_stats["total"] = sum(current_stats[key] for key in families)

        last_stats = {
            "started_at": current_stats.get("started_at", now),
            "total": current_stats.get(family, 0),
            "pro": current_stats["pro"] if family == "pro" else 0,
            "flash": current_stats["flash"] if family == "flash" else 0,
            "other": current_stats["other"] if family == "other" else 0,
            "updated_at": current_stats.get("updated_at", now),
            "ended_at": now,
            "cooldown_family": family,
        }

        new_stats = {
            "started_at": now,
            "pro": current_stats["pro"],
            "flash": current_stats["flash"],
            "other": current_stats["other"],
        }
        new_stats[family] = 0
        new_stats["total"] = sum(new_stats[key] for key in families)

        return json.dumps(new_stats), json.dumps(last_stats)

    async def get_today_stats_by_model(self, mode: Optional[str] = None) -> Dict[str, Any]:
        """获取今日按模型家族汇总的统计。

        返回:
            {
              "date": "yyyy-mm-dd",
              "by_family": {
                  "2.5-pro": {"success": x, "failure": y, "total": z, "rpm": r},
                  ...
              },
              "totals": {"success": x, "failure": y, "total": z, "rpm": r}
            }
        """
        self._ensure_initialized()
        today = _today_beijing_str()
        # RPM 取最近 60 秒（包含当前分钟）
        now_ts = int(time.time())
        from_ts = now_ts - 60

        try:
            async with self._pool.acquire() as conn:
                if mode:
                    daily_rows = await conn.fetch(
                        """
                        SELECT model_family,
                               COALESCE(success_count, 0) AS s,
                               COALESCE(failure_count, 0) AS f
                        FROM daily_model_stats
                        WHERE date = $1 AND mode = $2
                        """,
                        today, mode,
                    )
                    minute_rows = await conn.fetch(
                        """
                        SELECT model_family, COALESCE(SUM(count), 0) AS rpm
                        FROM minute_model_stats
                        WHERE minute_ts >= $1 AND mode = $2
                        GROUP BY model_family
                        """,
                        from_ts, mode,
                    )
                else:
                    daily_rows = await conn.fetch(
                        """
                        SELECT model_family,
                               SUM(success_count) AS s,
                               SUM(failure_count) AS f
                        FROM daily_model_stats
                        WHERE date = $1
                        GROUP BY model_family
                        """,
                        today,
                    )
                    minute_rows = await conn.fetch(
                        """
                        SELECT model_family, COALESCE(SUM(count), 0) AS rpm
                        FROM minute_model_stats
                        WHERE minute_ts >= $1
                        GROUP BY model_family
                        """,
                        from_ts,
                    )

            rpm_map = {r["model_family"]: int(r["rpm"] or 0) for r in minute_rows}
            by_family: Dict[str, Dict[str, int]] = {}
            tot_s = tot_f = 0
            for r in daily_rows:
                fam = r["model_family"]
                s = int(r["s"] or 0)
                f = int(r["f"] or 0)
                by_family[fam] = {
                    "success": s,
                    "failure": f,
                    "total": s + f,
                    "rpm": rpm_map.get(fam, 0),
                }
                tot_s += s
                tot_f += f

            # 没今日调用但当前 RPM>0 的模型也展示
            for fam, rpm in rpm_map.items():
                if fam not in by_family:
                    by_family[fam] = {"success": 0, "failure": 0, "total": 0, "rpm": rpm}

            tot_rpm = sum(rpm_map.values())
            return {
                "date": today,
                "mode": mode,
                "by_family": by_family,
                "totals": {
                    "success": tot_s,
                    "failure": tot_f,
                    "total": tot_s + tot_f,
                    "rpm": tot_rpm,
                },
            }
        except Exception as e:
            log.error(f"get_today_stats_by_model failed: {e}")
            return {
                "date": today,
                "mode": mode,
                "by_family": {},
                "totals": {"success": 0, "failure": 0, "total": 0, "rpm": 0},
                "error": str(e),
            }

    async def cleanup_minute_stats(self, keep_minutes: int = 1440) -> int:
        """清理超过 keep_minutes 分钟（默认 24h）的分钟统计数据。"""
        self._ensure_initialized()
        try:
            cutoff = int(time.time()) - max(60, int(keep_minutes) * 60)
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM minute_model_stats WHERE minute_ts < $1",
                    cutoff,
                )
            # asyncpg 返回 'DELETE n'
            try:
                return int(str(result).split()[-1])
            except Exception:
                return 0
        except Exception as e:
            log.error(f"cleanup_minute_stats failed: {e}")
            return 0
