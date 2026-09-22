"""
Antigravity API Client - Handles communication with Google's Antigravity API
处理与 Google Antigravity API 的通信
"""

import asyncio
import copy
import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable, Tuple

from fastapi import Response
from config import (
    get_antigravity_api_url,
    get_antigravity_stream2nostream,
    get_auto_ban_error_codes,
    get_delayed_hedge_enabled,
    get_delayed_hedge_timeout,
)
from log import log

from src.credential_manager import credential_manager
from src.httpx_client import stream_post_async, post_async
from src.models import Model, model_to_dict
from src.utils import ANTIGRAVITY_USER_AGENT, normalize_antigravity_model_alias
from src.converter.antigravity_fix import (
    normalize_antigravity_cooldown_key,
    normalize_antigravity_model_name,
)

# 导入共同的基础功能
from src.api.utils import (
    handle_error_with_retry,
    get_retry_config,
    record_api_call_success,
    record_api_call_error,
    parse_and_log_cooldown,
    collect_streaming_response,
    upgrade_short_antigravity_rate_limit_cooldown,
)

# ==================== 全局凭证管理器 ====================

# 使用全局单例 credential_manager，自动初始化


def antigravity_stats_model_name(model_name: str) -> str:
    """返回用于调用统计的真实模型名，与共享额度/CD键严格分离。"""
    aliased = normalize_antigravity_model_alias(model_name)
    return normalize_antigravity_model_name(aliased)


async def _maybe_upgrade_short_rate_limit_cooldown(
    error_text: str,
    cooldown_until: Optional[float],
    auth_headers: Dict[str, str],
    cooldown_key: str,
) -> Optional[float]:
    """短 RATE_LIMIT CD时补查原始额度，将其升级到共享族真实重置时间。"""
    import time

    if not error_text:
        return cooldown_until
    if cooldown_until is not None and cooldown_until >= time.time() + 60:
        return cooldown_until
    try:
        error_data = json.loads(error_text)
        details = (error_data.get("error") or error_data).get("details") or []
        if not any(
            isinstance(detail, dict) and detail.get("reason") == "RATE_LIMIT_EXCEEDED"
            for detail in details
        ):
            return cooldown_until
        antigravity_url = await get_antigravity_api_url()
        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},
            headers=auth_headers,
            timeout=30.0,
        )
        if response.status_code != 200:
            return cooldown_until
        quota_models = (response.json() or {}).get("models", {})
        upgraded = upgrade_short_antigravity_rate_limit_cooldown(
            error_data, cooldown_until, quota_models, cooldown_key
        )
        if upgraded and (not cooldown_until or upgraded > cooldown_until + 60):
            log.info(
                f"[ANTIGRAVITY] 短限流CD已升级到共享额度重置时间: "
                f"model_name={cooldown_key}, cooldown_until={datetime.fromtimestamp(upgraded, timezone.utc).isoformat()}"
            )
        return upgraded
    except Exception as exc:
        log.warning(f"[ANTIGRAVITY] 短限流CD补查额度失败，保留原CD: {exc}")
        return cooldown_until


def _extract_first_user_text(request_payload: Dict[str, Any]) -> str:
    contents = request_payload.get("contents", [])
    if not isinstance(contents, list):
        return ""
    for content in contents:
        if not isinstance(content, dict) or content.get("role") != "user":
            continue
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if isinstance(part, dict) and part.get("text"):
                return str(part["text"])
    return ""


def _generate_request_id() -> str:
    """生成完整格式的 requestId，对齐参考实现:
    agent/{uuid}/{毫秒时间戳}/{trajectory_id}/{step}
    """
    trajectory_id = str(uuid.uuid4())
    step = 1
    ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"agent/{uuid.uuid4()}/{ms}/{trajectory_id}/{step}"


def _build_labels(model: str, trajectory_id: str, step: int) -> Dict[str, str]:
    used_claude = "claude" in model.lower()
    return {
        "last_step_index": str(step),
        "model_enum": model,
        "trajectory_id": trajectory_id,
        "used_claude": str(used_claude).lower(),
        "used_claude_conservative": str(used_claude).lower(),
    }


def _should_forward_antigravity_header(header_name: str) -> bool:
    normalized = header_name.strip().lower()
    if not normalized:
        return False
    if normalized.startswith("x-b3-"):
        return True
    return normalized in {
        "accept-language",
        "traceparent",
        "tracestate",
        "x-cloud-trace-context",
        "x-goog-api-client",
        "x-goog-request-params",
        "x-goog-user-project",
        "x-request-id",
    }


def _sanitize_antigravity_headers(extra_headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not extra_headers:
        return {}
    sanitized: Dict[str, str] = {}
    for key, value in extra_headers.items():
        if _should_forward_antigravity_header(key):
            sanitized[key] = value
    return sanitized


async def wrap_cli_request(
    gemini_request: Dict[str, Any],
    model: str,
    project_id: str,
    enable_credit: bool = False,
) -> Tuple[Dict[str, Any], str]:
    """
    将 Gemini 格式请求包装成 Antigravity CLI 格式。
    返回 (payload, request_id)。
    """
    inner = copy.deepcopy(gemini_request)
    first_user_text = _extract_first_user_text(inner)

    # 移除 safetySettings（CLI 不发送）
    inner.pop("safetySettings", None)

    # 注入 sessionId
    session_id = str(inner.get("sessionId") or "").strip()
    if not session_id:
        if first_user_text:
            digest = hashlib.sha256(first_user_text.encode("utf-8")).digest()
            session_id_val = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
            session_id = f"-{session_id_val}"
        else:
            session_id = f"-{uuid.uuid4().int % 9_000_000_000_000_000_000}"
        inner["sessionId"] = session_id

    # 注入 labels
    inner["labels"] = _build_labels(model, session_id, 1)

    # toolConfig 默认 VALIDATED
    tool_config = inner.get("toolConfig") or {}
    func_config = tool_config.get("functionCallingConfig") or {}
    func_config["mode"] = "VALIDATED"
    tool_config["functionCallingConfig"] = func_config
    inner["toolConfig"] = tool_config

    request_id = _generate_request_id()

    payload = {
        "project": project_id,
        "requestId": request_id,
        "request": inner,
        "model": model,
        "userAgent": "antigravity",
        "requestType": "agent",
    }
    if enable_credit:
        payload["enabledCreditTypes"] = ["GOOGLE_ONE_AI"]
    return payload, request_id


# ==================== 辅助函数 ====================

def build_antigravity_headers(
    access_token: str,
    extra_headers: Optional[Dict[str, str]] = None,
    model_name: str = "",
) -> Dict[str, str]:
    """构建 Antigravity CLI API 请求头。"""
    headers = {
        "User-Agent": ANTIGRAVITY_USER_AGENT,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "requestId": f"req-{uuid.uuid4()}",
    }

    for key, value in _sanitize_antigravity_headers(extra_headers).items():
        headers.setdefault(key, value)

    # 根据模型名称判断 request_type
    if model_name:
        if "image" in model_name.lower():
            headers["requestType"] = "image_gen"
        else:
            headers["requestType"] = "agent"

    return headers


def _is_retryable_status(status_code: int, disable_error_codes: List[int]) -> bool:
    """统一判断是否属于可重试状态码。"""
    return status_code in (429, 503) or status_code in disable_error_codes


async def _switch_credential_for_retry(
    *,
    next_cred_task: Optional[asyncio.Task],
    retry_interval: float,
    refresh_credential_fast: Callable[[], Any],
    apply_cred_result: Callable[[Tuple[str, Dict[str, Any]]], bool],
    log_prefix: str,
) -> Tuple[bool, Optional[asyncio.Task]]:
    """优先使用预热凭证，失败后退回同步刷新。"""
    if next_cred_task is not None:
        try:
            cred_result = await next_cred_task
            next_cred_task = None
            if cred_result and apply_cred_result(cred_result):
                await asyncio.sleep(retry_interval)
                return True, next_cred_task
        except Exception as e:
            log.warning(f"{log_prefix} 预热凭证任务失败: {e}")
            next_cred_task = None

    await asyncio.sleep(retry_interval)
    if await refresh_credential_fast():
        return True, next_cred_task

    return False, next_cred_task


# ==================== 新的流式和非流式请求函数 ====================

async def stream_request(
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
):
    """Run a delayed backup stream when the primary has no first output."""
    if not await get_delayed_hedge_enabled():
        async for chunk in _stream_request_once(body, native, headers):
            yield chunk
        return

    from src.delayed_hedge import hedge_stream, is_empty_stream_item
    from src.storage_adapter import get_storage_adapter

    used_files = []
    model_name = body.get("model", "")
    cooldown_model_name = normalize_antigravity_model_name(model_name)
    cooldown_key = normalize_antigravity_cooldown_key(model_name)
    primary_credential = await credential_manager.get_valid_credential(
        mode="antigravity", model_name=cooldown_key
    )
    if not primary_credential:
        yield Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json",
        )
        return
    used_files.append(primary_credential[0])

    def primary_factory():
        return _stream_request_once(
            body,
            native,
            headers,
            excluded_filenames=used_files,
            selected_file=used_files,
            initial_credential=primary_credential,
            on_upstream_start=lambda: record_event("primary_started"),
            upstream_started_event=primary_started,
        )

    def backup_factory():
        return _stream_request_once(
            body,
            native,
            headers,
            excluded_filenames=used_files,
            selected_file=used_files,
            on_upstream_start=lambda: record_event("backup_started"),
        )

    async def record_event(event: str):
        storage = await get_storage_adapter()
        backend = getattr(storage, "_backend", None)
        if backend and hasattr(backend, "record_hedge_event"):
            await backend.record_hedge_event("antigravity", event)

    primary_started = asyncio.Event()

    async for chunk in hedge_stream(
        primary_factory,
        backup_factory,
        delay_seconds=await get_delayed_hedge_timeout(),
        is_success=lambda item: not isinstance(item, Response),
        is_ignorable=lambda item: item in (b"", "") or is_empty_stream_item(item),
        on_event=record_event,
        primary_started=primary_started,
    ):
        yield chunk


async def _stream_request_once(
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
    excluded_filenames: Optional[List[str]] = None,
    selected_file: Optional[List[str]] = None,
    on_upstream_start: Optional[Callable[[], Any]] = None,
    upstream_started_event: Optional[asyncio.Event] = None,
    initial_credential: Optional[Tuple[str, Dict[str, Any]]] = None,
):
    """
    流式请求函数

    Args:
        body: 请求体
        native: 是否返回原生bytes流，False则返回str流
        headers: 额外的请求头

    Yields:
        Response对象（错误时）或 bytes流/str流（成功时）
    """
    model_name = body.get("model", "")
    cooldown_model_name = normalize_antigravity_model_name(model_name)
    cooldown_key = normalize_antigravity_cooldown_key(model_name)
    stats_model_name = antigravity_stats_model_name(model_name)

    # 1. 获取有效凭证
    cred_result = initial_credential or await credential_manager.get_valid_credential(
        mode="antigravity",
        model_name=cooldown_key,
        excluded_filenames=excluded_filenames,
    )

    if not cred_result:
        # 如果返回值是None，直接返回错误500
        log.error("[ANTIGRAVITY STREAM] 当前无可用凭证")
        yield Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json"
        )
        return

    current_file, credential_data = cred_result
    if selected_file is not None and current_file not in selected_file:
        selected_file.append(current_file)
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = credential_data.get("project_id", "")
    enable_credit = bool(credential_data.get("enable_credit", False))

    if not access_token:
        log.error(f"[ANTIGRAVITY STREAM] No access token in credential: {current_file}")
        yield Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )
        return

    # 2. 构建URL和请求头
    antigravity_url = await get_antigravity_api_url()
    target_url = f"{antigravity_url}/v1internal:streamGenerateContent?alt=sse"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id, enable_credit)

    # 3. 调用stream_post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务
    content_yielded = False  # 已向下游输出正文后，禁止异常时完整重试

    async def record_upstream_attempt():
        if on_upstream_start is not None:
            result = on_upstream_start()
            if asyncio.iscoroutine(result):
                await result

    def mark_response_started(status_code: int):
        if status_code == 200 and upstream_started_event is not None:
            upstream_started_event.set()

    def apply_credential_to_request(credential_data: Dict[str, Any]) -> bool:
        """完整应用重试凭证，禁止跨账号继承信用额度开关。"""
        nonlocal access_token, project_id, enable_credit, auth_headers, final_payload
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        enable_credit = bool(credential_data.get("enable_credit", False))
        if not access_token or not project_id:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        if enable_credit:
            final_payload["enabledCreditTypes"] = ["GOOGLE_ONE_AI"]
        else:
            final_payload.pop("enabledCreditTypes", None)
        return True

    # 内部函数：快速更新凭证，同时同步项目与信用额度开关
    async def refresh_credential_fast():
        nonlocal current_file
        cred_result = await credential_manager.get_valid_credential(
            mode="antigravity",
            model_name=cooldown_key,
            excluded_filenames=excluded_filenames,
        )
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        return apply_credential_to_request(credential_data) or None

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file
        current_file, credential_data = cred_result
        return apply_credential_to_request(credential_data)

    for attempt in range(max_retries + 1):
        # 每次真实上游尝试都换新追踪标识，避免跨凭证/重试复用。
        auth_headers["requestId"] = f"req-{uuid.uuid4()}"
        final_payload["requestId"] = _generate_request_id()
        success_recorded = False  # 标记是否已记录成功
        need_retry = False  # 标记是否需要重试

        try:
            async for chunk in stream_post_async(
                url=target_url,
                body=final_payload,
                native=native,
                headers=auth_headers,
                on_request_attempt=record_upstream_attempt,
                on_response_started=mark_response_started,
            ):
                # 判断是否是Response对象
                if isinstance(chunk, Response):
                    status_code = chunk.status_code
                    last_error_response = chunk  # 记录最后一次错误

                    # 缓存错误解析结果,避免重复decode
                    error_body = None
                    try:
                        error_body = chunk.body.decode('utf-8') if isinstance(chunk.body, bytes) else str(chunk.body)
                    except Exception:
                        error_body = ""

                    # 如果错误码是429、503或者在禁用码当中，做好记录后进行重试
                    if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                        log.warning(f"[ANTIGRAVITY STREAM] 流式请求失败 (status={status_code}), 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")

                        # 解析冷却时间
                        cooldown_until = None
                        if (status_code == 429 or status_code == 503) and error_body:
                            try:
                                cooldown_until = await parse_and_log_cooldown(error_body, mode="antigravity")
                                cooldown_until = await _maybe_upgrade_short_rate_limit_cooldown(
                                    error_body, cooldown_until, auth_headers, cooldown_key
                                )
                            except Exception:
                                pass

                        # 预热下一个凭证
                        if next_cred_task is None and attempt < max_retries:
                            next_cred_task = asyncio.create_task(
                                credential_manager.get_valid_credential(
                                    mode="antigravity",
                                    model_name=cooldown_key,
                                    excluded_filenames=excluded_filenames,
                                )
                            )

                        # 记录错误并切换凭证
                        await record_api_call_error(
                            credential_manager, current_file, status_code,
                            cooldown_until, mode="antigravity", model_name=stats_model_name,
                            error_message=error_body
                        )

                        # 检查是否应该重试
                        should_retry = await handle_error_with_retry(
                            credential_manager, status_code, current_file,
                            retry_config["retry_enabled"], attempt, max_retries, retry_interval,
                            mode="antigravity"
                        )

                        if should_retry and attempt < max_retries:
                            need_retry = True
                            break  # 跳出内层循环，准备重试
                        else:
                            # 不重试，直接返回原始错误
                            log.error(f"[ANTIGRAVITY STREAM] 达到最大重试次数或不应重试，返回原始错误")
                            yield chunk
                            return
                    else:
                        # 错误码不在禁用码当中，直接返回，无需重试
                        log.error(f"[ANTIGRAVITY STREAM] 流式请求失败，非重试错误码 (status={status_code}), 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")
                        await record_api_call_error(
                            credential_manager, current_file, status_code,
                            None, mode="antigravity", model_name=stats_model_name,
                            error_message=error_body
                        )
                        yield chunk
                        return
                else:
                    # 不是Response，说明是真流，直接yield返回
                    # 只在第一个chunk时记录成功
                    if not success_recorded:
                        await record_api_call_success(
                            credential_manager, current_file, mode="antigravity", model_name=stats_model_name
                        )
                        success_recorded = True
                        log.debug(f"[ANTIGRAVITY STREAM] 开始接收流式响应，模型: {model_name}")

                    # 记录原始chunk内容（用于调试）
                    content_yielded = True
                    if isinstance(chunk, bytes):
                        log.debug(f"[ANTIGRAVITY STREAM RAW] chunk(bytes): {chunk}")
                    else:
                        log.debug(f"[ANTIGRAVITY STREAM RAW] chunk(str): {chunk}")

                    yield chunk

            # 流式请求完成，检查结果
            if success_recorded:
                log.debug(f"[ANTIGRAVITY STREAM] 流式响应完成，模型: {model_name}")
                return
            elif not need_retry:
                # 没有收到任何数据（空回复），需要重试
                log.warning(f"[ANTIGRAVITY STREAM] 收到空回复，无任何内容，凭证: {current_file}")
                await record_api_call_error(
                    credential_manager, current_file, 200,
                    None, mode="antigravity", model_name=stats_model_name,
                    error_message="Empty response from API"
                )
                
                if attempt < max_retries:
                    need_retry = True
                else:
                    log.error(f"[ANTIGRAVITY STREAM] 空回复达到最大重试次数")
                    yield Response(
                        content=json.dumps({"error": "服务返回空回复"}),
                        status_code=500,
                        media_type="application/json"
                    )
                    return
            
            # 统一处理重试
            if need_retry:
                log.info(f"[ANTIGRAVITY STREAM] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                switched, next_cred_task = await _switch_credential_for_retry(
                    next_cred_task=next_cred_task,
                    retry_interval=retry_interval,
                    refresh_credential_fast=refresh_credential_fast,
                    apply_cred_result=apply_cred_result,
                    log_prefix="[ANTIGRAVITY STREAM]",
                )
                if not switched:
                    log.error("[ANTIGRAVITY STREAM] 重试时无可用凭证或令牌")
                    yield Response(
                        content=json.dumps({"error": "当前无可用凭证"}),
                        status_code=500,
                        media_type="application/json"
                    )
                    return
                continue  # 重试

        except Exception as e:
            log.error(f"[ANTIGRAVITY STREAM] 流式请求异常: {e}, 凭证: {current_file}")
            if content_yielded:
                log.error("[ANTIGRAVITY STREAM] 已向下游输出正文，禁止异常后完整重试，结束当前流")
                raise
            if attempt < max_retries:
                log.info(f"[ANTIGRAVITY STREAM] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                await asyncio.sleep(retry_interval)
                continue
            else:
                # 所有重试都失败，返回最后一次的错误（如果有）
                log.error(f"[ANTIGRAVITY STREAM] 所有重试均失败，最后异常: {e}")
                if last_error_response:
                    yield last_error_response
                else:
                    # 如果没有记录到错误响应，返回500错误
                    yield Response(
                        content=json.dumps({"error": f"流式请求异常: {str(e)}"}),
                        status_code=500,
                        media_type="application/json"
                    )
                return

    # 所有重试均已耗尽（for循环正常结束），返回最后记录的错误
    log.error("[ANTIGRAVITY STREAM] 所有重试均失败")
    if last_error_response:
        yield last_error_response
    else:
        yield Response(
            content=json.dumps({"error": "请求失败，所有重试均已耗尽"}),
            status_code=429,
            media_type="application/json"
        )


async def non_stream_request(
    body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Response:
    """
    非流式请求函数

    Args:
        body: 请求体
        headers: 额外的请求头

    Returns:
        Response对象
    """
    # 检查是否启用流式收集模式
    if await get_antigravity_stream2nostream():
        log.debug("[ANTIGRAVITY] 使用流式收集模式实现非流式请求")

        # 调用stream_request获取流
        stream = stream_request(body=body, native=False, headers=headers)

        # 收集流式响应
        # stream_request是一个异步生成器，可能yield Response（错误）或流数据
        # collect_streaming_response会自动处理这两种情况
        return await collect_streaming_response(stream)

    # 否则使用传统非流式模式
    log.debug("[ANTIGRAVITY] 使用传统非流式模式")

    model_name = body.get("model", "")
    cooldown_model_name = normalize_antigravity_model_name(model_name)
    cooldown_key = normalize_antigravity_cooldown_key(model_name)
    stats_model_name = antigravity_stats_model_name(model_name)

    # 1. 获取有效凭证
    cred_result = await credential_manager.get_valid_credential(
        mode="antigravity", model_name=cooldown_key
    )

    if not cred_result:
        # 如果返回值是None，直接返回错误500
        log.error("[ANTIGRAVITY] 当前无可用凭证")
        return Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json"
        )

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = credential_data.get("project_id", "")
    enable_credit = bool(credential_data.get("enable_credit", False))

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        return Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )

    # 2. 构建URL和请求头
    antigravity_url = await get_antigravity_api_url()
    target_url = f"{antigravity_url}/v1internal:generateContent"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id, enable_credit)

    # 3. 调用post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务

    def apply_credential_to_request(credential_data: Dict[str, Any]) -> bool:
        """完整应用重试凭证，禁止跨账号继承信用额度开关。"""
        nonlocal access_token, project_id, enable_credit, auth_headers, final_payload
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        enable_credit = bool(credential_data.get("enable_credit", False))
        if not access_token or not project_id:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        if enable_credit:
            final_payload["enabledCreditTypes"] = ["GOOGLE_ONE_AI"]
        else:
            final_payload.pop("enabledCreditTypes", None)
        return True

    # 内部函数：快速更新凭证，同时同步项目与信用额度开关
    async def refresh_credential_fast():
        nonlocal current_file
        cred_result = await credential_manager.get_valid_credential(
            mode="antigravity", model_name=cooldown_key
        )
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        return apply_credential_to_request(credential_data) or None

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file
        current_file, credential_data = cred_result
        return apply_credential_to_request(credential_data)

    for attempt in range(max_retries + 1):
        # 每次真实上游尝试都换新追踪标识，避免跨凭证/重试复用。
        auth_headers["requestId"] = f"req-{uuid.uuid4()}"
        final_payload["requestId"] = _generate_request_id()
        need_retry = False  # 标记是否需要重试
        
        try:
            response = await post_async(
                url=target_url,
                json=final_payload,
                headers=auth_headers
            )

            status_code = response.status_code

            # 成功
            if status_code == 200:
                # 检查是否为空回复
                if not response.content or len(response.content) == 0:
                    log.warning(f"[ANTIGRAVITY] 收到200响应但内容为空，凭证: {current_file}")
                    
                    # 记录错误
                    await record_api_call_error(
                        credential_manager, current_file, 200,
                        None, mode="antigravity", model_name=stats_model_name,
                        error_message="Empty response from API"
                    )
                    
                    if attempt < max_retries:
                        need_retry = True
                    else:
                        log.error(f"[ANTIGRAVITY] 空回复达到最大重试次数")
                        return Response(
                            content=json.dumps({"error": "服务返回空回复"}),
                            status_code=500,
                            media_type="application/json"
                        )
                else:
                    # 正常响应
                    await record_api_call_success(
                        credential_manager, current_file, mode="antigravity", model_name=stats_model_name
                    )
                    return Response(
                        content=response.content,
                        status_code=200,
                        headers=dict(response.headers)
                    )

            # 失败 - 记录最后一次错误
            if status_code != 200:
                last_error_response = Response(
                    content=response.content,
                    status_code=status_code,
                    headers=dict(response.headers)
                )

                # 判断是否需要重试
                # 缓存错误文本,避免重复解析
                error_text = ""
                try:
                    error_text = response.text
                except Exception:
                    pass

                if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                    log.warning(f"[ANTIGRAVITY] 非流式请求失败 (status={status_code}), 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")

                    # 解析冷却时间
                    cooldown_until = None
                    if (status_code == 429 or status_code == 503) and error_text:
                        try:
                            cooldown_until = await parse_and_log_cooldown(error_text, mode="antigravity")
                            cooldown_until = await _maybe_upgrade_short_rate_limit_cooldown(
                                error_text, cooldown_until, auth_headers, cooldown_key
                            )
                        except Exception:
                            pass

                    # 并行预热下一个凭证,不阻塞当前处理
                    if next_cred_task is None and attempt < max_retries:
                        next_cred_task = asyncio.create_task(
                            credential_manager.get_valid_credential(
                                mode="antigravity", model_name=cooldown_key
                            )
                        )

                    # 记录错误并切换凭证
                    await record_api_call_error(
                        credential_manager, current_file, status_code,
                        cooldown_until, mode="antigravity", model_name=stats_model_name,
                        error_message=error_text
                    )

                    # 检查是否应该重试
                    should_retry = await handle_error_with_retry(
                        credential_manager, status_code, current_file,
                        retry_config["retry_enabled"], attempt, max_retries, retry_interval,
                        mode="antigravity"
                    )

                    if should_retry and attempt < max_retries:
                        need_retry = True
                    else:
                        # 不重试，直接返回原始错误
                        log.error(f"[ANTIGRAVITY] 达到最大重试次数或不应重试，返回原始错误")
                        return last_error_response
                else:
                    # 错误码不在禁用码当中，直接返回，无需重试
                    log.error(f"[ANTIGRAVITY] 非流式请求失败，非重试错误码 (status={status_code}), 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")
                    await record_api_call_error(
                        credential_manager, current_file, status_code,
                        None, mode="antigravity", model_name=stats_model_name,
                        error_message=error_text
                    )
                    return last_error_response
            
            # 统一处理重试
            if need_retry:
                log.info(f"[ANTIGRAVITY] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                switched, next_cred_task = await _switch_credential_for_retry(
                    next_cred_task=next_cred_task,
                    retry_interval=retry_interval,
                    refresh_credential_fast=refresh_credential_fast,
                    apply_cred_result=apply_cred_result,
                    log_prefix="[ANTIGRAVITY]",
                )
                if not switched:
                    log.error("[ANTIGRAVITY] 重试时无可用凭证或令牌")
                    return Response(
                        content=json.dumps({"error": "当前无可用凭证"}),
                        status_code=500,
                        media_type="application/json"
                    )
                continue  # 重试

        except Exception as e:
            log.error(f"[ANTIGRAVITY] 非流式请求异常: {e}, 凭证: {current_file}")
            if attempt < max_retries:
                log.info(f"[ANTIGRAVITY] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                await asyncio.sleep(retry_interval)
                continue
            else:
                # 所有重试都失败，返回最后一次的错误（如果有）或500错误
                log.error(f"[ANTIGRAVITY] 所有重试均失败，最后异常: {e}")
                if last_error_response:
                    return last_error_response
                else:
                    return Response(
                        content=json.dumps({"error": f"非流式请求异常: {str(e)}"}),
                        status_code=500,
                        media_type="application/json"
                    )

    # 所有重试都失败，返回最后一次的原始错误（如果有）或500错误
    log.error("[ANTIGRAVITY] 所有重试均失败")
    if last_error_response:
        return last_error_response
    else:
        return Response(
            content=json.dumps({"error": "所有重试均失败"}),
            status_code=500,
            media_type="application/json"
        )


# ==================== 模型和配额查询 ====================

async def fetch_available_models() -> List[Dict[str, Any]]:
    """
    获取可用模型列表，返回符合 OpenAI API 规范的格式
    
    Returns:
        模型列表，格式为字典列表（用于兼容现有代码）
        
    Raises:
        返回空列表如果获取失败
    """
    # 获取凭证管理器和可用凭证
    cred_result = await credential_manager.get_valid_credential(mode="antigravity")
    if not cred_result:
        log.error("[ANTIGRAVITY] No valid credentials available for fetching models")
        return []

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        return []

    # 构建请求头
    headers = build_antigravity_headers(access_token, model_name="agent")

    try:
        # 使用 POST 请求获取模型列表
        antigravity_url = await get_antigravity_api_url()

        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},  # 空的请求体
            headers=headers
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY] Raw models response: {json.dumps(data, ensure_ascii=False)[:500]}")

            # 转换为 OpenAI 格式的模型列表，使用 Model 类
            model_list = []
            current_timestamp = int(datetime.now(timezone.utc).timestamp())

            if 'models' in data and isinstance(data['models'], dict):
                # 遍历模型字典
                for model_id in data['models'].keys():
                    model = Model(
                        id=model_id,
                        object='model',
                        created=current_timestamp,
                        owned_by='google'
                    )
                    model_list.append(model_to_dict(model))
            # 添加额外的 claude-sonnet-4-6-thinking 模型
            if "claude-sonnet-4-6" in data.get('models', {}):
                model = Model(
                    id='claude-sonnet-4-6-thinking',
                    object='model',
                    created=current_timestamp,
                    owned_by='google'
                )
                model_list.append(model_to_dict(model))
            # 添加额外的 claude-opus-4-6 模型
            if "claude-opus-4-6-thinking" in data.get('models', {}):
                claude_opus_model = Model(
                    id='claude-opus-4-6',
                    object='model',
                    created=current_timestamp,
                    owned_by='google'
                )
                model_list.append(model_to_dict(claude_opus_model))

            log.info(f"[ANTIGRAVITY] Fetched {len(model_list)} available models")
            return model_list
        else:
            log.error(f"[ANTIGRAVITY] Failed to fetch models ({response.status_code}): {response.text[:500]}")
            return []

    except Exception as e:
        import traceback
        log.error(f"[ANTIGRAVITY] Failed to fetch models: {e}")
        log.error(f"[ANTIGRAVITY] Traceback: {traceback.format_exc()}")
        return []


async def fetch_quota_info(access_token: str) -> Dict[str, Any]:
    """
    获取指定凭证的额度信息
    
    Args:
        access_token: Antigravity 访问令牌
        
    Returns:
        包含额度信息的字典，格式为：
        {
            "success": True/False,
            "models": {
                "model_name": {
                    "remaining": 0.95,
                    "resetTime": "12-20 10:30",
                    "resetTimeRaw": "2025-12-20T02:30:00Z"
                }
            },
            "error": "错误信息" (仅在失败时)
        }
    """

    headers = build_antigravity_headers(access_token, model_name="agent")

    try:
        antigravity_url = await get_antigravity_api_url()

        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},
            headers=headers,
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY QUOTA] Raw response: {json.dumps(data, ensure_ascii=False)[:500]}")

            quota_info = {}

            if 'models' in data and isinstance(data['models'], dict):
                for model_id, model_data in data['models'].items():
                    if isinstance(model_data, dict) and 'quotaInfo' in model_data:
                        quota = model_data['quotaInfo']
                        remaining = quota.get('remainingFraction', 0)
                        reset_time_raw = quota.get('resetTime', '')

                        # 转换为北京时间
                        reset_time_beijing = 'N/A'
                        if reset_time_raw:
                            try:
                                utc_date = datetime.fromisoformat(reset_time_raw.replace('Z', '+00:00'))
                                # 转换为北京时间 (UTC+8)
                                from datetime import timedelta
                                beijing_date = utc_date + timedelta(hours=8)
                                reset_time_beijing = beijing_date.strftime('%m-%d %H:%M')
                            except Exception as e:
                                log.warning(f"[ANTIGRAVITY QUOTA] Failed to parse reset time: {e}")

                        quota_info[model_id] = {
                            "remaining": remaining,
                            "resetTime": reset_time_beijing,
                            "resetTimeRaw": reset_time_raw
                        }

            return {
                "success": True,
                "models": quota_info
            }
        else:
            log.error(f"[ANTIGRAVITY QUOTA] Failed to fetch quota ({response.status_code}): {response.text[:500]}")
            return {
                "success": False,
                "error": f"API返回错误: {response.status_code}"
            }

    except Exception as e:
        import traceback
        log.error(f"[ANTIGRAVITY QUOTA] Failed to fetch quota: {e}")
        log.error(f"[ANTIGRAVITY QUOTA] Traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e)
        }