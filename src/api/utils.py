"""
Base API Client - 共用的 API 客户端基础功能
提供错误处理、自动封禁、重试逻辑等共同功能
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import Response

from config import (
    get_auto_ban_enabled,
    get_auto_ban_error_codes,
    get_retry_429_enabled,
    get_retry_429_interval,
    get_retry_429_max_retries,
)
from log import log
from src.credential_manager import CredentialManager


# ==================== 错误检查与处理 ====================

async def check_should_auto_ban(status_code: int) -> bool:
    """
    检查是否应该触发自动封禁
    
    Args:
        status_code: HTTP状态码
        
    Returns:
        bool: 是否应该触发自动封禁
    """
    return (
        await get_auto_ban_enabled()
        and status_code in await get_auto_ban_error_codes()
    )


async def handle_auto_ban(
    credential_manager: CredentialManager,
    status_code: int,
    credential_name: str,
    mode: str = "geminicli"
) -> None:
    """
    处理自动封禁：直接禁用凭证
    
    Args:
        credential_manager: 凭证管理器实例
        status_code: HTTP状态码
        credential_name: 凭证名称
        mode: 模式（geminicli 或 antigravity）
    """
    if credential_manager and credential_name:
        log.warning(
            f"[{mode.upper()} AUTO_BAN] Status {status_code} triggers auto-ban for credential: {credential_name}"
        )
        await credential_manager.set_cred_disabled(
            credential_name, True, mode=mode
        )


async def handle_error_with_retry(
    credential_manager: CredentialManager,
    status_code: int,
    credential_name: str,
    retry_enabled: bool,
    attempt: int,
    max_retries: int,
    retry_interval: float,
    mode: str = "geminicli"
) -> bool:
    """
    统一处理错误和重试逻辑

    仅在以下情况下进行自动重试:
    1. 429错误(速率限制)
    2. 503错误(服务不可用)
    3. 500错误(服务临时不可用)
    4. 导致凭证封禁的错误(AUTO_BAN_ERROR_CODES配置)

    Args:
        credential_manager: 凭证管理器实例
        status_code: HTTP状态码
        credential_name: 凭证名称
        retry_enabled: 是否启用重试
        attempt: 当前重试次数
        max_retries: 最大重试次数
        retry_interval: 重试间隔
        mode: 模式（geminicli 或 antigravity）
        
    Returns:
        bool: True表示需要继续重试，False表示不需要重试
    """
    # 优先检查自动封禁
    should_auto_ban = await check_should_auto_ban(status_code)

    if should_auto_ban:
        # 触发自动封禁
        await handle_auto_ban(credential_manager, status_code, credential_name, mode)

        # 自动封禁后，仍然尝试重试（会在下次循环中自动获取新凭证）
        if retry_enabled and attempt < max_retries:
            log.info(
                f"[{mode.upper()} RETRY] Retrying with next credential after auto-ban "
                f"(status {status_code}, attempt {attempt + 1}/{max_retries})"
            )
            await asyncio.sleep(retry_interval)
            return True
        return False

    # 如果不触发自动封禁，仅对429、503和500错误进行重试
    if status_code in (429, 500, 503) and retry_enabled and attempt < max_retries:
        log.info(
            f"[{mode.upper()} RETRY] {status_code} error encountered, retrying "
            f"(attempt {attempt + 1}/{max_retries})"
        )
        await asyncio.sleep(retry_interval)
        return True

    # 其他错误不进行重试
    return False


# ==================== 重试配置获取 ====================

async def get_retry_config() -> Dict[str, Any]:
    """
    获取重试配置
    
    Returns:
        包含重试配置的字典
    """
    return {
        "retry_enabled": await get_retry_429_enabled(),
        "max_retries": await get_retry_429_max_retries(),
        "retry_interval": await get_retry_429_interval(),
    }


# ==================== API调用结果记录 ====================

async def record_api_call_success(
    credential_manager: CredentialManager,
    credential_name: str,
    mode: str = "geminicli",
    model_name: Optional[str] = None
) -> None:
    """
    记录API调用成功
    
    Args:
        credential_manager: 凭证管理器实例
        credential_name: 凭证名称
        mode: 模式（geminicli 或 antigravity）
        model_name: 模型名称（用于模型级CD）
    """
    if credential_manager and credential_name:
        await credential_manager.record_api_call_result(
            credential_name, True, mode=mode, model_name=model_name
        )


async def record_api_call_error(
    credential_manager: CredentialManager,
    credential_name: str,
    status_code: int,
    cooldown_until: Optional[float] = None,
    mode: str = "geminicli",
    model_name: Optional[str] = None,
    error_message: Optional[str] = None
) -> None:
    """
    记录API调用错误

    Args:
        credential_manager: 凭证管理器实例
        credential_name: 凭证名称
        status_code: HTTP状态码
        cooldown_until: 冷却截止时间（Unix时间戳）
        mode: 模式（geminicli 或 antigravity）
        model_name: 模型名称（用于模型级CD）
        error_message: 错误信息（可选）
    """
    if credential_manager and credential_name:
        await credential_manager.record_api_call_result(
            credential_name,
            False,
            status_code,
            cooldown_until=cooldown_until,
            mode=mode,
            model_name=model_name,
            error_message=error_message
        )


# ==================== 429错误处理 ====================

async def parse_and_log_cooldown(
    error_text: str,
    mode: str = "geminicli"
) -> Optional[float]:
    """
    解析并记录冷却时间

    Args:
        error_text: 错误响应文本
        mode: 模式（geminicli 或 antigravity）

    Returns:
        冷却截止时间（Unix时间戳），如果解析失败则返回None
    """
    try:
        error_data = json.loads(error_text)
        cooldown_until = parse_quota_reset_timestamp(error_data, mode=mode)
        if cooldown_until:
            log.info(
                f"[{mode.upper()}] 检测到quota冷却时间: "
                f"{datetime.fromtimestamp(cooldown_until, timezone.utc).isoformat()}"
            )
            return cooldown_until
    except Exception as parse_err:
        log.debug(f"[{mode.upper()}] Failed to parse cooldown time: {parse_err}")
    return None


# ==================== 流式响应收集 ====================

async def collect_streaming_response(stream_generator) -> Response:
    """
    将Gemini流式响应收集为一条完整的非流式响应

    Args:
        stream_generator: 流式响应生成器，产生 "data: {json}" 格式的行或Response对象

    Returns:
        Response: 合并后的完整响应对象

    Example:
        >>> async for line in stream_generator:
        ...     # line format: "data: {...}" or Response object
        >>> response = await collect_streaming_response(stream_generator)
    """
    # 初始化响应结构
    merged_response = {
        "response": {
            "candidates": [{
                "content": {
                    "parts": [],
                    "role": "model"
                },
                "finishReason": None,
                "safetyRatings": [],
                "citationMetadata": None
            }],
            "usageMetadata": {
                "promptTokenCount": 0,
                "candidatesTokenCount": 0,
                "totalTokenCount": 0
            }
        }
    }

    collected_text = []  # 用于收集文本内容
    collected_thought_text = []  # 用于收集思维链内容
    collected_other_parts = []  # 用于收集其他类型的parts（图片、文件、工具调用等）
    collected_tool_parts_count = 0  # 记录工具调用相关part数量
    has_data = False
    line_count = 0

    log.debug("[STREAM COLLECTOR] Starting to collect streaming response")

    try:
        async for line in stream_generator:
            line_count += 1

            # 如果收到的是Response对象（错误），直接返回
            if isinstance(line, Response):
                log.debug(f"[STREAM COLLECTOR] 收到错误Response，状态码: {line.status_code}")
                return line

            # 处理 bytes 类型
            if isinstance(line, bytes):
                line_str = line.decode('utf-8', errors='ignore')
                log.debug(f"[STREAM COLLECTOR] Processing bytes line {line_count}: {line_str[:200] if line_str else 'empty'}")
            elif isinstance(line, str):
                line_str = line
                log.debug(f"[STREAM COLLECTOR] Processing line {line_count}: {line_str[:200] if line_str else 'empty'}")
            else:
                log.debug(f"[STREAM COLLECTOR] Skipping non-string/bytes line: {type(line)}")
                continue

            # 解析流式数据行
            if not line_str.startswith("data: "):
                log.debug(f"[STREAM COLLECTOR] Skipping line without 'data: ' prefix: {line_str[:100]}")
                continue

            raw = line_str[6:].strip()
            if raw == "[DONE]":
                log.debug("[STREAM COLLECTOR] Received [DONE] marker")
                break

            try:
                log.debug(f"[STREAM COLLECTOR] Parsing JSON: {raw[:200]}")
                chunk = json.loads(raw)
                has_data = True
                log.debug(f"[STREAM COLLECTOR] Chunk keys: {chunk.keys() if isinstance(chunk, dict) else type(chunk)}")

                # 提取响应对象
                response_obj = chunk.get("response", {})
                if not response_obj:
                    log.debug("[STREAM COLLECTOR] No 'response' key in chunk, trying direct access")
                    response_obj = chunk  # 尝试直接使用chunk

                candidates = response_obj.get("candidates", [])
                log.debug(f"[STREAM COLLECTOR] Found {len(candidates)} candidates")
                if not candidates:
                    log.debug(f"[STREAM COLLECTOR] No candidates in chunk, chunk structure: {list(chunk.keys()) if isinstance(chunk, dict) else type(chunk)}")
                    continue

                candidate = candidates[0]

                # 收集文本内容
                content = candidate.get("content", {})
                parts = content.get("parts", [])
                log.debug(f"[STREAM COLLECTOR] Processing {len(parts)} parts from candidate")

                for part in parts:
                    if not isinstance(part, dict):
                        continue

                    # 优先保留工具调用相关 part（functionCall / functionResponse）
                    # 避免在 stream2nostream 模式下工具调用丢失
                    if "functionCall" in part or "functionResponse" in part or "function_call" in part:
                        collected_other_parts.append(part)
                        collected_tool_parts_count += 1
                        log.debug(f"[STREAM COLLECTOR] Collected tool part: {list(part.keys())}")
                        continue

                    # 处理文本内容
                    text = part.get("text", "")
                    if text:
                        # 区分普通文本和思维链
                        if part.get("thought", False):
                            collected_thought_text.append(text)
                            log.debug(f"[STREAM COLLECTOR] Collected thought text: {text[:100]}")
                        else:
                            collected_text.append(text)
                            log.debug(f"[STREAM COLLECTOR] Collected regular text: {text[:100]}")
                    # 处理非文本内容（图片、文件等）
                    elif "inlineData" in part or "fileData" in part or "executableCode" in part or "codeExecutionResult" in part:
                        collected_other_parts.append(part)
                        log.debug(f"[STREAM COLLECTOR] Collected non-text part: {list(part.keys())}")

                # 收集其他信息（使用最后一个块的值）
                if candidate.get("finishReason"):
                    merged_response["response"]["candidates"][0]["finishReason"] = candidate["finishReason"]

                if candidate.get("safetyRatings"):
                    merged_response["response"]["candidates"][0]["safetyRatings"] = candidate["safetyRatings"]

                if candidate.get("citationMetadata"):
                    merged_response["response"]["candidates"][0]["citationMetadata"] = candidate["citationMetadata"]

                # 更新使用元数据
                usage = response_obj.get("usageMetadata", {})
                if usage:
                    merged_response["response"]["usageMetadata"].update(usage)

            except json.JSONDecodeError as e:
                log.debug(f"[STREAM COLLECTOR] Failed to parse JSON chunk: {e}")
                continue
            except Exception as e:
                log.debug(f"[STREAM COLLECTOR] Error processing chunk: {e}")
                continue

    except Exception as e:
        log.error(f"[STREAM COLLECTOR] Error collecting stream after {line_count} lines: {e}")
        return Response(
            content=json.dumps({"error": f"收集流式响应失败: {str(e)}"}),
            status_code=500,
            media_type="application/json"
        )

    log.debug(f"[STREAM COLLECTOR] Finished iteration, has_data={has_data}, line_count={line_count}")

    # 如果没有收集到任何数据，返回错误
    if not has_data:
        log.error(f"[STREAM COLLECTOR] No data collected from stream after {line_count} lines")
        return Response(
            content=json.dumps({"error": "No data collected from stream"}),
            status_code=500,
            media_type="application/json"
        )

    # 组装最终的parts
    final_parts = []

    # 先添加思维链内容（如果有）
    if collected_thought_text:
        final_parts.append({
            "text": "".join(collected_thought_text),
            "thought": True
        })

    # 再添加普通文本内容
    if collected_text:
        final_parts.append({
            "text": "".join(collected_text)
        })

    # 添加其他类型的parts（图片、文件等）
    final_parts.extend(collected_other_parts)

    # 如果没有任何内容，添加空文本
    if not final_parts:
        final_parts.append({"text": ""})

    merged_response["response"]["candidates"][0]["content"]["parts"] = final_parts

    log.info(
        f"[STREAM COLLECTOR] Collected {len(collected_text)} text chunks, "
        f"{len(collected_thought_text)} thought chunks, {len(collected_other_parts)} other parts "
        f"(tool parts: {collected_tool_parts_count})"
    )

    # 去掉嵌套的 "response" 包装（Antigravity格式 -> 标准Gemini格式）
    if "response" in merged_response and "candidates" not in merged_response:
        log.debug(f"[STREAM COLLECTOR] 展开response包装")
        merged_response = merged_response["response"]

    # 返回纯JSON格式
    return Response(
        content=json.dumps(merged_response, ensure_ascii=False).encode('utf-8'),
        status_code=200,
        headers={},
        media_type="application/json"
    )


RESOURCE_EXHAUSTED_COOLDOWN_HOURS = 4  # RESOURCE_EXHAUSTED 错误的默认冷却时间（小时）

# Google 返回的 reason 中表示"服务端容量挤爆"（不是用户额度耗尽）的标记。
# 这种情况通常几秒就恢复，不应该用 4h 兜底锁死，否则会把可用凭证误判为冷却。
CAPACITY_EXHAUSTED_REASONS = {
    "MODEL_CAPACITY_EXHAUSTED",
    "RESOURCE_EXHAUSTED",   # 个别响应只给了笼统的 reason
    "NO_CAPACITY_AVAILABLE",
}


def upgrade_short_antigravity_rate_limit_cooldown(
    error_response: dict,
    cooldown_until: Optional[float],
    quota_models: dict,
    cooldown_key: str,
    *,
    now: Optional[float] = None,
) -> Optional[float]:
    """将异常短 RATE_LIMIT CD升级到共享额度族的真实 resetTime。"""
    import time as _time
    from datetime import datetime as _dt
    from src.converter.antigravity_fix import normalize_antigravity_cooldown_key

    current = _time.time() if now is None else float(now)
    if cooldown_until is not None and float(cooldown_until) >= current + 60:
        return cooldown_until
    error_obj = (error_response or {}).get("error", error_response or {})
    details = error_obj.get("details") or []
    reasons = {
        detail.get("reason")
        for detail in details
        if isinstance(detail, dict) and detail.get("reason")
    }
    if "RATE_LIMIT_EXCEEDED" not in reasons:
        return cooldown_until

    resets = []
    for model_name, model_data in (quota_models or {}).items():
        if normalize_antigravity_cooldown_key(model_name) != cooldown_key:
            continue
        quota = (model_data or {}).get("quotaInfo") or {}
        raw = quota.get("resetTime") or ""
        if not raw:
            continue
        try:
            ts = _dt.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        if ts >= current + 60:
            resets.append(ts)
    return max(resets) if resets else cooldown_until


def parse_quota_reset_timestamp(error_response: dict, mode: str = "geminicli") -> Optional[float]:
    """
    从Google API错误响应中提取quota重置时间戳

    Args:
        error_response: Google API返回的错误响应字典

    Returns:
        Unix时间戳（秒），如果无法解析则返回None

    设计要点：
      - QUOTA_EXHAUSTED（用户每日额度耗尽）走 4h 兜底
      - MODEL_CAPACITY_EXHAUSTED（Google 服务端排队挤爆，常见于 preview 模型）
        没有可解析的 reset 字段时直接返回 None，不锁冷却。
        这种 429 通常 2-3 秒就恢复，由调度器重试一次即可。

    示例错误响应:
    {
      "error": {
        "code": 429,
        "message": "You have exhausted your capacity...",
        "status": "RESOURCE_EXHAUSTED",
        "details": [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "QUOTA_EXHAUSTED",
            "metadata": {
              "quotaResetTimeStamp": "2025-11-30T14:57:24Z",
              "quotaResetDelay": "13h19m1.20964964s"
            }
          }
        ]
      }
    }
    """
    try:
        error_obj = error_response.get("error", {})

        details = error_obj.get("details", [])

        # 收集 ErrorInfo 中的 reason，用于决定是否兜底
        reasons = set()
        for detail in details:
            if detail.get("@type") == "type.googleapis.com/google.rpc.ErrorInfo":
                reason = detail.get("reason")
                if reason:
                    reasons.add(reason)

        # 优先级 1: 显式的 quotaResetTimeStamp
        for detail in details:
            if detail.get("@type") == "type.googleapis.com/google.rpc.ErrorInfo":
                reset_timestamp_str = detail.get("metadata", {}).get("quotaResetTimeStamp")

                if reset_timestamp_str:
                    if reset_timestamp_str.endswith("Z"):
                        reset_timestamp_str = reset_timestamp_str.replace("Z", "+00:00")

                    reset_dt = datetime.fromisoformat(reset_timestamp_str)
                    if reset_dt.tzinfo is None:
                        reset_dt = reset_dt.replace(tzinfo=timezone.utc)

                    return reset_dt.astimezone(timezone.utc).timestamp()

        # 优先级 2: quotaResetDelay (相对延迟)
        import time as _time
        import re as _re
        for detail in details:
            if detail.get("@type") == "type.googleapis.com/google.rpc.ErrorInfo":
                delay_str = detail.get("metadata", {}).get("quotaResetDelay")
                if delay_str:
                    # 解析格式: "13h19m1.20964964s" 或 "0s"
                    m = _re.match(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?", delay_str)
                    if m and any(m.groups()):
                        h = int(m.group(1) or 0)
                        mi = int(m.group(2) or 0)
                        sec = float(m.group(3) or 0)
                        delay_sec = h * 3600 + mi * 60 + sec
                        if delay_sec > 0:
                            return _time.time() + delay_sec

        # 优先级 3: 仅当确认是"用户每日额度耗尽"才兜底 4h。
        # 容量挤爆类 reason（MODEL_CAPACITY_EXHAUSTED 等）不锁冷却。
        if error_obj.get("status") == "RESOURCE_EXHAUSTED":
            if reasons & CAPACITY_EXHAUSTED_REASONS and "QUOTA_EXHAUSTED" not in reasons:
                # 服务端容量问题：让调度器自然重试，不写入冷却
                return None
            if mode.lower() == "antigravity" and not reasons:
                # 裸 Antigravity 429 没有原因或重置点时，不误锁整个额度族。
                return None
            return _time.time() + RESOURCE_EXHAUSTED_COOLDOWN_HOURS * 3600

        return None

    except Exception:
        return None
