"""
凭证管理路由模块 - 处理 /creds/* 相关的HTTP请求
"""

import asyncio
import hashlib
import io
import json
import os
import time
import zipfile
from typing import Any, List, Optional, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Response, Body
from fastapi.responses import JSONResponse

from log import log
from src.credential_manager import credential_manager
from src.models import (
    CredFileActionRequest,
    CredFileBatchActionRequest,
    CredFileBatchTestRequest,
    RefreshTokenAddRequest,
    RefreshTokenBatchAddRequest,
)
from src.storage_adapter import get_storage_adapter
from src.utils import (
    verify_panel_token,
    GEMINICLI_USER_AGENT,
    ANTIGRAVITY_USER_AGENT,
    ANTIGRAVITY_CLIENT_ID,
    ANTIGRAVITY_CLIENT_SECRET,
    CLIENT_ID as UTILS_GEMINI_CLIENT_ID,
    CLIENT_SECRET as UTILS_GEMINI_CLIENT_SECRET,
)
from src.api.antigravity import fetch_quota_info
from src.api.utils import is_license_error, apply_probe_error_classification
from src.api.utils import check_should_auto_ban
from src.google_oauth_api import Credentials, fetch_project_id_and_tier, get_user_projects, select_default_project, enable_required_apis, ensure_geminicli_project, validate_geminicli_project
from src.httpx_client import post_async
from src.task_manager import create_managed_task
from config import get_code_assist_endpoint, get_antigravity_api_url, get_oauth_proxy_url
from datetime import datetime, timedelta, timezone
from .utils import validate_mode


# 创建路由器
router = APIRouter(prefix="/creds", tags=["credentials"])


# =============================================================================
# 工具函数 (Helper Functions)
# =============================================================================


async def extract_json_files_from_zip(zip_file: UploadFile) -> List[dict]:
    """从ZIP文件中提取JSON文件"""
    try:
        # 读取ZIP文件内容
        zip_content = await zip_file.read()

        # 不限制ZIP文件大小，只在处理时控制文件数量

        files_data = []

        with zipfile.ZipFile(io.BytesIO(zip_content), "r") as zip_ref:
            # 获取ZIP中的所有文件
            file_list = zip_ref.namelist()
            json_files = [
                f for f in file_list if f.endswith(".json") and not f.startswith("__MACOSX/")
            ]

            if not json_files:
                raise HTTPException(status_code=400, detail="ZIP文件中没有找到JSON文件")

            log.info(f"从ZIP文件 {zip_file.filename} 中找到 {len(json_files)} 个JSON文件")

            for json_filename in json_files:
                try:
                    # 读取JSON文件内容
                    with zip_ref.open(json_filename) as json_file:
                        content = json_file.read()

                        try:
                            content_str = content.decode("utf-8")
                        except UnicodeDecodeError:
                            log.warning(f"跳过编码错误的文件: {json_filename}")
                            continue

                        # 使用原始文件名（去掉路径）
                        filename = os.path.basename(json_filename)
                        files_data.append({"filename": filename, "content": content_str})

                except Exception as e:
                    log.warning(f"处理ZIP中的文件 {json_filename} 时出错: {e}")
                    continue

        log.info(f"成功从ZIP文件中提取 {len(files_data)} 个有效的JSON文件")
        return files_data

    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="无效的ZIP文件格式")
    except Exception as e:
        log.error(f"处理ZIP文件失败: {e}")
        raise HTTPException(status_code=500, detail=f"处理ZIP文件失败: {str(e)}")


async def clear_all_model_cooldowns_for_credential(
    storage_adapter: Any,
    filename: str,
    mode: str,
) -> None:
    """清空指定凭证的所有模型冷却（后端支持时执行）。"""
    try:
        cleared = await storage_adapter._backend.clear_all_model_cooldowns(filename, mode=mode)
        if not cleared:
            log.warning(f"清空模型CD失败或凭证不存在: {filename} (mode={mode})")
    except Exception as e:
        log.warning(f"清空模型CD时出错: {filename} (mode={mode}), error={e}")


async def upload_credentials_common(
    files: List[UploadFile], mode: str = "geminicli"
) -> JSONResponse:
    """批量上传凭证文件的通用函数"""
    mode = validate_mode(mode)

    if not files:
        raise HTTPException(status_code=400, detail="请选择要上传的文件")

    # 检查文件数量限制
    if len(files) > 100:
        raise HTTPException(
            status_code=400, detail=f"文件数量过多，最多支持100个文件，当前：{len(files)}个"
        )

    files_data = []
    for file in files:
        # 检查文件类型：支持JSON和ZIP
        if file.filename.endswith(".zip"):
            zip_files_data = await extract_json_files_from_zip(file)
            files_data.extend(zip_files_data)
            log.info(f"从ZIP文件 {file.filename} 中提取了 {len(zip_files_data)} 个JSON文件")

        elif file.filename.endswith(".json"):
            # 处理单个JSON文件 - 流式读取
            content_chunks = []
            while True:
                chunk = await file.read(8192)
                if not chunk:
                    break
                content_chunks.append(chunk)

            content = b"".join(content_chunks)
            try:
                content_str = content.decode("utf-8")
            except UnicodeDecodeError:
                raise HTTPException(
                    status_code=400, detail=f"文件 {file.filename} 编码格式不支持"
                )

            files_data.append({"filename": file.filename, "content": content_str})
        else:
            raise HTTPException(
                status_code=400, detail=f"文件 {file.filename} 格式不支持，只支持JSON和ZIP文件"
            )



    batch_size = 1000
    all_results = []
    total_success = 0

    for i in range(0, len(files_data), batch_size):
        batch_files = files_data[i : i + batch_size]

        async def process_single_file(file_data):
            try:
                filename = file_data["filename"]
                # 确保文件名只保存basename，避免路径问题
                filename = os.path.basename(filename)
                content_str = file_data["content"]
                credential_data = json.loads(content_str)

                # 根据凭证类型调用不同的添加方法
                if mode == "antigravity":
                    await credential_manager.add_antigravity_credential(filename, credential_data)
                else:
                    await credential_manager.add_credential(filename, credential_data)

                log.debug(f"成功上传 {mode} 凭证文件: {filename}")
                return {"filename": filename, "status": "success", "message": "上传成功"}

            except json.JSONDecodeError as e:
                return {
                    "filename": file_data["filename"],
                    "status": "error",
                    "message": f"JSON格式错误: {str(e)}",
                }
            except Exception as e:
                return {
                    "filename": file_data["filename"],
                    "status": "error",
                    "message": f"处理失败: {str(e)}",
                }

        log.info(f"开始并发处理 {len(batch_files)} 个 {mode} 文件...")
        concurrent_tasks = [process_single_file(file_data) for file_data in batch_files]
        batch_results = await asyncio.gather(*concurrent_tasks, return_exceptions=True)

        processed_results = []
        batch_uploaded_count = 0
        for result in batch_results:
            if isinstance(result, Exception):
                processed_results.append(
                    {
                        "filename": "unknown",
                        "status": "error",
                        "message": f"处理异常: {str(result)}",
                    }
                )
            else:
                processed_results.append(result)
                if result["status"] == "success":
                    batch_uploaded_count += 1

        all_results.extend(processed_results)
        total_success += batch_uploaded_count

        batch_num = (i // batch_size) + 1
        total_batches = (len(files_data) + batch_size - 1) // batch_size
        log.info(
            f"批次 {batch_num}/{total_batches} 完成: 成功 "
            f"{batch_uploaded_count}/{len(batch_files)} 个 {mode} 文件"
        )

    if total_success > 0:
        return JSONResponse(
            content={
                "uploaded_count": total_success,
                "total_count": len(files_data),
                "results": all_results,
                "message": f"批量上传完成: 成功 {total_success}/{len(files_data)} 个 {mode} 文件",
            }
        )
    else:
        raise HTTPException(status_code=400, detail=f"没有 {mode} 文件上传成功")


async def get_creds_status_common(
    offset: int, limit: int, status_filter: str, mode: str = "geminicli",
    error_code_filter: str = None, cooldown_filter: str = None, preview_filter: str = None, tier_filter: str = None, remark_filter: str = None
) -> JSONResponse:
    """获取凭证文件状态的通用函数"""
    mode = validate_mode(mode)
    # 验证分页参数
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset 必须大于等于 0")
    if limit not in [20, 50, 100, 200, 500, 1000]:
        raise HTTPException(status_code=400, detail="limit 只能是 20、50、100、200、500 或 1000")
    if status_filter not in ["all", "enabled", "disabled", "permanent_disabled", "licensable"]:
        raise HTTPException(status_code=400, detail="status_filter 只能是 all、enabled、disabled、permanent_disabled 或 licensable")
    if cooldown_filter and cooldown_filter not in ["all", "in_cooldown", "no_cooldown", "pro_no_cooldown", "flash_no_cooldown", "claude_no_cooldown"]:
        raise HTTPException(status_code=400, detail="cooldown_filter 只能是 all、in_cooldown、no_cooldown、pro_no_cooldown、flash_no_cooldown 或 claude_no_cooldown")
    if preview_filter and preview_filter not in ["all", "preview", "no_preview"]:
        raise HTTPException(status_code=400, detail="preview_filter 只能是 all、preview 或 no_preview")
    if tier_filter and tier_filter not in ["all", "free", "pro", "ultra"]:
        raise HTTPException(status_code=400, detail="tier_filter 只能是 all、free、pro 或 ultra")
    if remark_filter is not None and len(remark_filter) > 64:
        raise HTTPException(status_code=400, detail="remark_filter 不能超过 64 个字符")

    storage_adapter = await get_storage_adapter()
    backend_info = await storage_adapter.get_backend_info()
    backend_type = backend_info.get("backend_type", "unknown")

    # 使用高性能的分页摘要查询
    result = await storage_adapter._backend.get_credentials_summary(
        offset=offset,
        limit=limit,
        status_filter=status_filter,
        mode=mode,
        error_code_filter=error_code_filter if error_code_filter and error_code_filter != "all" else None,
        cooldown_filter=cooldown_filter if cooldown_filter and cooldown_filter != "all" else None,
        preview_filter=preview_filter if preview_filter and preview_filter != "all" else None,
        tier_filter=tier_filter if tier_filter and tier_filter != "all" else None,
        remark_filter=remark_filter if remark_filter is not None and remark_filter != "__all__" else None
    )

    creds_list = []
    for summary in result["items"]:
        cred_info = {
            "filename": os.path.basename(summary["filename"]),
            "user_email": summary["user_email"],
            "disabled": summary["disabled"],
            "licensable": summary.get("licensable", False),
            "error_codes": summary["error_codes"],
            "last_success": summary["last_success"],
            "backend_type": backend_type,
            "model_cooldowns": summary.get("model_cooldowns", {}),
            "model_disabled": summary.get("model_disabled", {}),
            "tier": summary.get("tier", "pro"),
            "success_count": summary.get("success_count", 0),
            "failure_count": summary.get("failure_count", 0),
            "cycle_stats": summary.get("cycle_stats", {}),
            "last_cycle_stats": summary.get("last_cycle_stats", {}),
            "remark": summary.get("remark", ""),
        }

        if mode == "geminicli":
            cred_info["preview"] = summary.get("preview", True)
        else:
            cred_info["enable_credit"] = summary.get("enable_credit", False)

        creds_list.append(cred_info)

    return JSONResponse(content={
        "items": creds_list,
        "total": result["total"],
        "offset": offset,
        "limit": limit,
        "has_more": (offset + limit) < result["total"],
        "stats": result.get("stats", {"total": 0, "normal": 0, "disabled": 0, "licensable": 0}),
    })


async def download_all_creds_common(mode: str = "geminicli") -> Response:
    """打包下载所有凭证文件的通用函数"""
    mode = validate_mode(mode)
    zip_filename = "antigravity_credentials.zip" if mode == "antigravity" else "credentials.zip"

    storage_adapter = await get_storage_adapter()
    credential_filenames = await storage_adapter.list_credentials(mode=mode)

    if not credential_filenames:
        raise HTTPException(status_code=404, detail=f"没有找到 {mode} 凭证文件")

    log.info(f"开始打包 {len(credential_filenames)} 个 {mode} 凭证文件...")

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        success_count = 0
        for idx, filename in enumerate(credential_filenames, 1):
            try:
                credential_data = await storage_adapter.get_credential(filename, mode=mode)
                if credential_data:
                    content = json.dumps(credential_data, ensure_ascii=False, indent=2)
                    zip_file.writestr(os.path.basename(filename), content)
                    success_count += 1

                    if idx % 10 == 0:
                        log.debug(f"打包进度: {idx}/{len(credential_filenames)}")

            except Exception as e:
                log.warning(f"处理 {mode} 凭证文件 {filename} 时出错: {e}")
                continue

    log.info(f"打包完成: 成功 {success_count}/{len(credential_filenames)} 个文件")

    zip_buffer.seek(0)
    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={zip_filename}"},
    )


async def fetch_user_email_common(filename: str, mode: str = "geminicli") -> JSONResponse:
    """获取指定凭证文件用户邮箱的通用函数"""
    mode = validate_mode(mode)

    filename_only = os.path.basename(filename)
    if not filename_only.endswith(".json"):
        raise HTTPException(status_code=404, detail="无效的文件名")

    storage_adapter = await get_storage_adapter()
    credential_data = await storage_adapter.get_credential(filename_only, mode=mode)
    if not credential_data:
        raise HTTPException(status_code=404, detail="凭证文件不存在")

    email = await credential_manager.get_or_fetch_user_email(filename_only, mode=mode)

    if email:
        return JSONResponse(
            content={
                "filename": filename_only,
                "user_email": email,
                "message": "成功获取用户邮箱",
            }
        )
    else:
        return JSONResponse(
            content={
                "filename": filename_only,
                "user_email": None,
                "message": "无法获取用户邮箱，可能凭证已过期或权限不足",
            },
            status_code=400,
        )


async def refresh_all_user_emails_common(mode: str = "geminicli") -> JSONResponse:
    """刷新所有凭证文件用户邮箱的通用函数 - 只为没有邮箱的凭证获取

    利用 get_all_credential_states 批量获取状态
    """
    mode = validate_mode(mode)

    storage_adapter = await get_storage_adapter()

    # 一次性批量获取所有凭证的状态
    all_states = await storage_adapter.get_all_credential_states(mode=mode)

    results = []
    success_count = 0
    skipped_count = 0

    # 在内存中筛选出需要获取邮箱的凭证
    for filename, state in all_states.items():
        try:
            cached_email = state.get("user_email")

            if cached_email:
                # 已有邮箱，跳过获取
                skipped_count += 1
                results.append({
                    "filename": os.path.basename(filename),
                    "user_email": cached_email,
                    "success": True,
                    "skipped": True,
                })
                continue

            # 没有邮箱，尝试获取
            email = await credential_manager.get_or_fetch_user_email(filename, mode=mode)
            if email:
                success_count += 1
                results.append({
                    "filename": os.path.basename(filename),
                    "user_email": email,
                    "success": True,
                })
            else:
                results.append({
                    "filename": os.path.basename(filename),
                    "user_email": None,
                    "success": False,
                    "error": "无法获取邮箱",
                })
        except Exception as e:
            results.append({
                "filename": os.path.basename(filename),
                "user_email": None,
                "success": False,
                "error": str(e),
            })

    total_count = len(all_states)
    return JSONResponse(
        content={
            "success_count": success_count,
            "total_count": total_count,
            "skipped_count": skipped_count,
            "results": results,
            "message": f"成功获取 {success_count}/{total_count} 个邮箱地址，跳过 {skipped_count} 个已有邮箱的凭证",
        }
    )


async def deduplicate_credentials_by_email_common(mode: str = "geminicli") -> JSONResponse:
    """批量去重凭证文件的通用函数 - 删除邮箱相同的凭证（只保留一个）"""
    mode = validate_mode(mode)
    storage_adapter = await get_storage_adapter()

    try:
        duplicate_info = await storage_adapter._backend.get_duplicate_credentials_by_email(
            mode=mode
        )

        duplicate_groups = duplicate_info.get("duplicate_groups", [])
        no_email_files = duplicate_info.get("no_email_files", [])
        total_count = duplicate_info.get("total_count", 0)

        if not duplicate_groups:
            return JSONResponse(
                content={
                    "deleted_count": 0,
                    "kept_count": total_count,
                    "total_count": total_count,
                    "unique_emails_count": duplicate_info.get("unique_email_count", 0),
                    "no_email_count": len(no_email_files),
                    "duplicate_groups": [],
                    "delete_errors": [],
                    "message": "没有发现重复的凭证（相同邮箱）",
                }
            )

        # 执行删除操作
        deleted_count = 0
        delete_errors = []
        result_duplicate_groups = []

        for group in duplicate_groups:
            email = group["email"]
            kept_file = group["kept_file"]
            duplicate_files = group["duplicate_files"]

            deleted_files_in_group = []
            for filename in duplicate_files:
                try:
                    success = await credential_manager.remove_credential(filename, mode=mode)
                    if success:
                        deleted_count += 1
                        deleted_files_in_group.append(os.path.basename(filename))
                        log.info(f"去重删除凭证: {filename} (邮箱: {email}) (mode={mode})")
                    else:
                        delete_errors.append(f"{os.path.basename(filename)}: 删除失败")
                except Exception as e:
                    delete_errors.append(f"{os.path.basename(filename)}: {str(e)}")
                    log.error(f"去重删除凭证 {filename} 时出错: {e}")

            result_duplicate_groups.append({
                "email": email,
                "kept_file": os.path.basename(kept_file),
                "deleted_files": deleted_files_in_group,
                "duplicate_count": len(deleted_files_in_group),
            })

        kept_count = total_count - deleted_count

        return JSONResponse(
            content={
                "deleted_count": deleted_count,
                "kept_count": kept_count,
                "total_count": total_count,
                "unique_emails_count": duplicate_info.get("unique_email_count", 0),
                "no_email_count": len(no_email_files),
                "duplicate_groups": result_duplicate_groups,
                "delete_errors": delete_errors,
                "message": f"去重完成：删除 {deleted_count} 个重复凭证，保留 {kept_count} 个凭证（{duplicate_info.get('unique_email_count', 0)} 个唯一邮箱）",
            }
        )

    except Exception as e:
        log.error(f"批量去重凭证时出错: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "deleted_count": 0,
                "kept_count": 0,
                "total_count": 0,
                "message": f"去重操作失败: {str(e)}",
            }
        )


async def verify_credential_project_common(filename: str, mode: str = "geminicli") -> JSONResponse:
    """验证并重新获取凭证的project id的通用函数"""
    mode = validate_mode(mode)

    # 验证文件名
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="无效的文件名")


    storage_adapter = await get_storage_adapter()

    # 获取凭证数据
    credential_data = await storage_adapter.get_credential(filename, mode=mode)
    if not credential_data:
        raise HTTPException(status_code=404, detail="凭证不存在")

    # 创建凭证对象
    credentials = Credentials.from_dict(credential_data)

    # 确保token有效（自动刷新）
    token_refreshed = await credentials.refresh_if_needed()

    # 如果token被刷新了，更新存储
    if token_refreshed:
        log.info(f"Token已自动刷新: {filename} (mode={mode})")
        credential_data = credentials.to_dict()
        await storage_adapter.store_credential(filename, credential_data, mode=mode)

    # 重新获取project id（仅 antigravity 模式请求积分）
    if mode == "antigravity":
        api_base_url = await get_antigravity_api_url()
        user_agent = ANTIGRAVITY_USER_AGENT
        project_id, subscription_tier, credit_amount = await fetch_project_id_and_tier(
            access_token=credentials.access_token,
            user_agent=user_agent,
            api_base_url=api_base_url,
            include_credits=True,
        )
    else:
        # geminicli 模式：通过项目列表获取 project_id
        credit_amount = None
        subscription_tier = None
        user_projects = await get_user_projects(credentials)
        if user_projects:
            if len(user_projects) == 1:
                project_id = user_projects[0].get("projectId")
            else:
                project_id = await select_default_project(user_projects)
        else:
            project_id = None

        if project_id:
            log.info(f"正在为项目 {project_id} 启用必需的API服务...")
            try:
                await enable_required_apis(credentials, project_id)
            except Exception as e:
                log.warning(f"自动启用API服务失败: {e}")

    if project_id:
        credential_data["project_id"] = project_id

    if project_id or subscription_tier:
        await storage_adapter.store_credential(filename, credential_data, mode=mode)

        # 检验成功后自动解除禁用状态并清除错误码
        state_update = {
            "disabled": False,
            "error_codes": []
        }

        # 同步更新状态表中的 tier 字段
        state_update["tier"] = subscription_tier

        # 如果是 geminicli 模式，直接设置 preview=True
        if mode == "geminicli":
            state_update["preview"] = True

        await storage_adapter.update_credential_state(filename, state_update, mode=mode)

        log.info(f"检验 {mode} 凭证成功: {filename} - Project ID: {project_id}, Tier: {subscription_tier} - 已解除禁用并清除错误码")

        response_data = {
            "success": True,
            "filename": filename,
            "project_id": project_id,
            "subscription_tier": subscription_tier,
            "message": "检验成功！Project ID已更新，已解除禁用状态并清除错误码，403错误应该已恢复"
        }

        if mode == "antigravity" and credit_amount is not None:
            response_data["credit_amount"] = credit_amount

        return JSONResponse(content=response_data)
    else:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "filename": filename,
                "message": "检验失败：无法获取Project ID，请检查凭证是否有效"
            }
        )


# =============================================================================
# 路由处理函数 (Route Handlers)
# =============================================================================


@router.post("/upload")
async def upload_credentials(
    files: List[UploadFile] = File(...),
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """批量上传凭证文件"""
    try:
        mode = validate_mode(mode)
        return await upload_credentials_common(files, mode=mode)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量上传失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_creds_status(
    token: str = Depends(verify_panel_token),
    offset: int = 0,
    limit: int = 50,
    status_filter: str = "all",
    error_code_filter: str = "all",
    cooldown_filter: str = "all",
    preview_filter: str = "all",
    tier_filter: str = "all",
    remark_filter: str = "__all__",
    mode: str = "geminicli"
):
    """
    获取凭证文件的状态（轻量级摘要，不包含完整凭证数据，支持分页和状态筛选）

    Args:
        offset: 跳过的记录数（默认0）
        limit: 每页返回的记录数（默认50，可选：20, 50, 100, 200, 500, 1000）
        status_filter: 状态筛选（all=全部, enabled=仅启用, disabled=仅禁用）
        error_code_filter: 错误码筛选（all=全部, 或具体错误码如"400", "403"）
        cooldown_filter: 冷却状态筛选（all=全部, in_cooldown=冷却中, no_cooldown=未冷却）
        preview_filter: Preview筛选（all=全部, preview=支持preview, no_preview=不支持preview，仅geminicli模式有效）
        tier_filter: tier筛选（all=全部, free/pro/ultra）
        mode: 凭证模式（geminicli 或 antigravity）

    Returns:
        包含凭证列表、总数、分页信息的响应
    """
    try:
        mode = validate_mode(mode)
        return await get_creds_status_common(
            offset, limit, status_filter, mode=mode,
            error_code_filter=error_code_filter,
            cooldown_filter=cooldown_filter,
            preview_filter=preview_filter,
            tier_filter=tier_filter,
            remark_filter=remark_filter
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remark/{filename}")
async def update_cred_remark(
    filename: str,
    payload: dict = Body(...),
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """更新凭证备注/标签。"""
    try:
        mode = validate_mode(mode)
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")

        remark = str(payload.get("remark", "")).strip()
        if len(remark) > 64:
            raise HTTPException(status_code=400, detail="备注不能超过 64 个字符")

        storage_adapter = await get_storage_adapter()
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        updated = await storage_adapter.update_credential_state(filename, {"remark": remark}, mode=mode)
        if not updated:
            raise HTTPException(status_code=500, detail="备注保存失败")

        return JSONResponse(content={"message": "备注已保存", "filename": os.path.basename(filename), "remark": remark})
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"更新凭证备注失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail/{filename}")
async def get_cred_detail(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """
    按需获取单个凭证的详细数据（包含完整凭证内容）
    用于用户查看/编辑凭证详情
    """
    try:
        mode = validate_mode(mode)
        # 验证文件名
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")



        storage_adapter = await get_storage_adapter()
        backend_info = await storage_adapter.get_backend_info()
        backend_type = backend_info.get("backend_type", "unknown")

        # 获取凭证数据
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        # 获取状态信息
        file_status = await storage_adapter.get_credential_state(filename, mode=mode)
        if not file_status:
            file_status = {
                "error_codes": [],
                "disabled": False,
                "last_success": time.time(),
                "user_email": None,
            }

        result = {
            "status": file_status,
            "content": credential_data,
            "filename": os.path.basename(filename),
            "backend_type": backend_type,
            "user_email": file_status.get("user_email"),
            "model_cooldowns": file_status.get("model_cooldowns", {}),
        }

        if mode == "geminicli":
            result["preview"] = file_status.get("preview", True)
        else:
            result["enable_credit"] = file_status.get("enable_credit", False)

        if backend_type == "file" and os.path.exists(filename):
            result.update({
                "size": os.path.getsize(filename),
                "modified_time": os.path.getmtime(filename),
            })

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证详情失败 {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/action")
async def creds_action(
    request: CredFileActionRequest,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """对凭证文件执行操作（启用/禁用/删除/enable_credit开关）"""
    try:
        mode = validate_mode(mode)

        log.info(f"Received request: {request}")

        filename = request.filename
        action = request.action

        log.info(f"Performing action '{action}' on file: {filename} (mode={mode})")

        # 验证文件名
        if not filename.endswith(".json"):
            log.error(f"无效的文件名: {filename}（不是.json文件）")
            raise HTTPException(status_code=400, detail=f"无效的文件名: {filename}")

        # 获取存储适配器
        storage_adapter = await get_storage_adapter()

        # 对于删除操作，不需要检查凭证数据是否完整，只需检查条目是否存在
        # 对于其他操作，需要确保凭证数据存在且完整
        if action != "delete":
            # 检查凭证数据是否存在
            credential_data = await storage_adapter.get_credential(filename, mode=mode)
            if not credential_data:
                log.error(f"凭证未找到: {filename} (mode={mode})")
                raise HTTPException(status_code=404, detail="凭证文件不存在")

        if action == "enable":
            log.info(f"Web请求: 启用文件 {filename} (mode={mode})")
            result = await credential_manager.set_cred_disabled(filename, False, mode=mode)
            log.info(f"[WebRoute] set_cred_disabled 返回结果: {result}")
            if result:
                log.info(f"Web请求: 文件 {filename} 已成功启用 (mode={mode})")
                return JSONResponse(content={"message": f"已启用凭证文件 {os.path.basename(filename)}"})
            else:
                log.error(f"Web请求: 文件 {filename} 启用失败 (mode={mode})")
                raise HTTPException(status_code=500, detail="启用凭证失败，可能凭证不存在")

        elif action == "disable":
            log.info(f"Web请求: 禁用文件 {filename} (mode={mode})")
            result = await credential_manager.set_cred_disabled(filename, True, mode=mode)
            log.info(f"[WebRoute] set_cred_disabled 返回结果: {result}")
            if result:
                log.info(f"Web请求: 文件 {filename} 已成功禁用 (mode={mode})")
                return JSONResponse(content={"message": f"已禁用凭证文件 {os.path.basename(filename)}"})
            else:
                log.error(f"Web请求: 文件 {filename} 禁用失败 (mode={mode})")
                raise HTTPException(status_code=500, detail="禁用凭证失败，可能凭证不存在")

        elif action == "permanent_disable":
            log.info(f"Web请求: 永久禁用文件 {filename} (mode={mode})")
            result = await credential_manager.update_credential_state(
                filename, {"disabled": True, "permanent_disabled": True}, mode=mode
            )
            if result:
                return JSONResponse(content={"message": f"已永久禁用凭证文件 {os.path.basename(filename)}"})
            raise HTTPException(status_code=500, detail="永久禁用凭证失败，可能凭证不存在")

        elif action == "delete":
            try:
                # 使用 CredentialManager 删除凭证（包含队列/状态同步）
                success = await credential_manager.remove_credential(filename, mode=mode)
                if success:
                    log.info(f"通过管理器成功删除凭证: {filename} (mode={mode})")
                    return JSONResponse(
                        content={"message": f"已删除凭证文件 {os.path.basename(filename)}"}
                    )
                else:
                    raise HTTPException(status_code=500, detail="删除凭证失败")
            except Exception as e:
                log.error(f"删除凭证 {filename} 时出错: {e}")
                raise HTTPException(status_code=500, detail=f"删除文件失败: {str(e)}")

        elif action == "enable_credit":
            if mode != "antigravity":
                raise HTTPException(status_code=400, detail="enable_credit 仅支持 antigravity 模式")
            updated = await storage_adapter.update_credential_state(
                filename, {"enable_credit": True}, mode=mode
            )
            if updated:
                await clear_all_model_cooldowns_for_credential(storage_adapter, filename, mode)
                return JSONResponse(content={"message": f"已开启凭证信用额度模式 {os.path.basename(filename)}"})
            raise HTTPException(status_code=500, detail="开启信用额度模式失败，可能凭证不存在")

        elif action == "disable_credit":
            if mode != "antigravity":
                raise HTTPException(status_code=400, detail="disable_credit 仅支持 antigravity 模式")
            updated = await storage_adapter.update_credential_state(
                filename, {"enable_credit": False}, mode=mode
            )
            if updated:
                await clear_all_model_cooldowns_for_credential(storage_adapter, filename, mode)
                return JSONResponse(content={"message": f"已关闭凭证信用额度模式 {os.path.basename(filename)}"})
            raise HTTPException(status_code=500, detail="关闭信用额度模式失败，可能凭证不存在")

        elif action in ("disable_claude", "enable_claude"):
            if mode != "antigravity":
                raise HTTPException(status_code=400, detail="Claude 单独禁用仅支持 antigravity 模式")
            disabled = action == "disable_claude"
            state = await storage_adapter.get_credential_state(filename, mode=mode)
            model_disabled = dict(state.get("model_disabled") or {})
            model_disabled["claude"] = disabled
            updated = await storage_adapter.update_credential_state(
                filename, {"model_disabled": model_disabled}, mode=mode
            )
            if updated:
                label = "禁用" if disabled else "恢复"
                return JSONResponse(content={"message": f"已{label}该凭证的 Claude 通道 {os.path.basename(filename)}"})
            raise HTTPException(status_code=500, detail="更新 Claude 通道状态失败，可能凭证不存在")

        else:
            raise HTTPException(status_code=400, detail="无效的操作类型")

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"凭证文件操作失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch-action")
async def creds_batch_action(
    request: CredFileBatchActionRequest,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """批量对凭证文件执行操作（启用/禁用/删除/enable_credit开关）"""
    try:
        mode = validate_mode(mode)

        action = request.action
        filenames = request.filenames

        if not filenames:
            raise HTTPException(status_code=400, detail="文件名列表不能为空")

        log.info(f"对 {len(filenames)} 个文件执行批量操作 '{action}'")

        success_count = 0
        errors = []

        storage_adapter = await get_storage_adapter()

        for filename in filenames:
            try:
                # 验证文件名安全性
                if not filename.endswith(".json"):
                    errors.append(f"{filename}: 无效的文件类型")
                    continue

                # 对于删除操作，不需要检查凭证数据完整性
                # 对于其他操作，需要确保凭证数据存在
                if action != "delete":
                    credential_data = await storage_adapter.get_credential(filename, mode=mode)
                    if not credential_data:
                        errors.append(f"{filename}: 凭证不存在")
                        continue

                # 执行相应操作
                if action == "enable":
                    await credential_manager.set_cred_disabled(filename, False, mode=mode)
                    success_count += 1

                elif action == "disable":
                    await credential_manager.set_cred_disabled(filename, True, mode=mode)
                    success_count += 1

                elif action == "permanent_disable":
                    await credential_manager.update_credential_state(
                        filename, {"disabled": True, "permanent_disabled": True}, mode=mode
                    )
                    success_count += 1

                elif action == "delete":
                    try:
                        delete_success = await credential_manager.remove_credential(filename, mode=mode)
                        if delete_success:
                            success_count += 1
                            log.info(f"成功删除批量中的凭证: {filename}")
                        else:
                            errors.append(f"{filename}: 删除失败")
                            continue
                    except Exception as e:
                        errors.append(f"{filename}: 删除文件失败 - {str(e)}")
                        continue
                elif action == "enable_credit":
                    if mode != "antigravity":
                        errors.append(f"{filename}: enable_credit 仅支持 antigravity 模式")
                        continue
                    updated = await storage_adapter.update_credential_state(
                        filename, {"enable_credit": True}, mode=mode
                    )
                    if updated:
                        await clear_all_model_cooldowns_for_credential(storage_adapter, filename, mode)
                        success_count += 1
                    else:
                        errors.append(f"{filename}: 开启信用额度模式失败")
                        continue
                elif action == "disable_credit":
                    if mode != "antigravity":
                        errors.append(f"{filename}: disable_credit 仅支持 antigravity 模式")
                        continue
                    updated = await storage_adapter.update_credential_state(
                        filename, {"enable_credit": False}, mode=mode
                    )
                    if updated:
                        await clear_all_model_cooldowns_for_credential(storage_adapter, filename, mode)
                        success_count += 1
                    else:
                        errors.append(f"{filename}: 关闭信用额度模式失败")
                        continue
                elif action in ("disable_claude", "enable_claude"):
                    if mode != "antigravity":
                        errors.append(f"{filename}: Claude 单独禁用仅支持 antigravity 模式")
                        continue
                    state = await storage_adapter.get_credential_state(filename, mode=mode)
                    model_disabled = dict(state.get("model_disabled") or {})
                    model_disabled["claude"] = action == "disable_claude"
                    updated = await storage_adapter.update_credential_state(
                        filename, {"model_disabled": model_disabled}, mode=mode
                    )
                    if updated:
                        success_count += 1
                    else:
                        errors.append(f"{filename}: 更新 Claude 通道状态失败")
                        continue
                else:
                    errors.append(f"{filename}: 无效的操作类型")
                    continue

            except Exception as e:
                log.error(f"处理 {filename} 时出错: {e}")
                errors.append(f"{filename}: 处理失败 - {str(e)}")
                continue

        # 构建返回消息
        result_message = f"批量操作完成：成功处理 {success_count}/{len(filenames)} 个文件"
        if errors:
            result_message += "\n错误详情:\n" + "\n".join(errors)

        response_data = {
            "success_count": success_count,
            "total_count": len(filenames),
            "errors": errors,
            "message": result_message,
        }

        return JSONResponse(content=response_data)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量凭证文件操作失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download/{filename}")
async def download_cred_file(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """下载单个凭证文件"""
    try:
        mode = validate_mode(mode)
        # 验证文件名安全性
        if not filename.endswith(".json"):
            raise HTTPException(status_code=404, detail="无效的文件名")

        # 获取存储适配器
        storage_adapter = await get_storage_adapter()

        # 从存储系统获取凭证数据
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="文件不存在")

        # 转换为JSON字符串
        content = json.dumps(credential_data, ensure_ascii=False, indent=2)

        from fastapi.responses import Response

        return Response(
            content=content,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"下载凭证文件失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetch-email/{filename}")
async def fetch_user_email(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """获取指定凭证文件的用户邮箱地址"""
    try:
        mode = validate_mode(mode)
        return await fetch_user_email_common(filename, mode=mode)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取用户邮箱失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/refresh-all-emails")
async def refresh_all_user_emails(
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """刷新所有凭证文件的用户邮箱地址"""
    try:
        mode = validate_mode(mode)
        return await refresh_all_user_emails_common(mode=mode)
    except Exception as e:
        log.error(f"批量获取用户邮箱失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deduplicate-by-email")
async def deduplicate_credentials_by_email(
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """批量去重凭证文件 - 删除邮箱相同的凭证（只保留一个）"""
    try:
        mode = validate_mode(mode)
        return await deduplicate_credentials_by_email_common(mode=mode)
    except Exception as e:
        log.error(f"批量去重凭证失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download-all")
async def download_all_creds(
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """
    打包下载所有凭证文件（流式处理，按需加载每个凭证数据）
    只在实际下载时才加载完整凭证内容，最大化性能
    """
    try:
        mode = validate_mode(mode)
        return await download_all_creds_common(mode=mode)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"打包下载失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify-project/{filename}")
async def verify_credential_project(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """
    检验凭证的project id，重新获取project id
    检验成功可以使403错误恢复
    """
    try:
        mode = validate_mode(mode)
        return await verify_credential_project_common(filename, mode=mode)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"检验凭证Project ID失败 {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"检验失败: {str(e)}")


@router.get("/errors/{filename}")
async def get_credential_errors(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """
    获取指定凭证的错误信息（包含 error_codes 和 error_messages）

    Args:
        filename: 凭证文件名
        mode: 凭证模式（geminicli 或 antigravity）

    Returns:
        包含 error_codes 和 error_messages 的 JSON 响应
    """
    try:
        mode = validate_mode(mode)

        # 验证文件名
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")

        storage_adapter = await get_storage_adapter()

        # 检查后端是否支持 get_credential_errors 方法
        if not hasattr(storage_adapter._backend, 'get_credential_errors'):
            raise HTTPException(
                status_code=501,
                detail="当前存储后端不支持获取错误信息"
            )

        # 获取错误信息
        error_info = await storage_adapter._backend.get_credential_errors(filename, mode=mode)

        return JSONResponse(content=error_info)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证错误信息失败 {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quota/{filename}")
async def get_credential_quota(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """
    获取指定凭证的额度信息

    - geminicli: 调用 cloudcode-pa retrieveUserQuota（需 project_id）
    - antigravity: 调用 fetchAvailableModels
    """
    try:
        mode = validate_mode(mode)
        # 验证文件名
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")


        storage_adapter = await get_storage_adapter()

        # 获取凭证数据
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        # 使用 Credentials 对象自动处理 token 刷新
        from src.google_oauth_api import Credentials

        creds = Credentials.from_dict(credential_data)

        # 自动刷新 token（如果需要）
        await creds.refresh_if_needed()

        # 如果 token 被刷新了，更新存储
        updated_data = creds.to_dict()
        if updated_data != credential_data:
            log.info(f"Token已自动刷新: {filename}")
            await storage_adapter.store_credential(filename, updated_data, mode=mode)
            credential_data = updated_data

        # 获取访问令牌
        access_token = credential_data.get("access_token") or credential_data.get("token")
        if not access_token:
            raise HTTPException(status_code=400, detail="凭证中没有访问令牌")

        # 按 mode 分发获取额度
        if mode == "antigravity":
            quota_info = await fetch_quota_info(access_token)
        else:
            from src.api.geminicli import fetch_geminicli_quota_info
            project_id = credential_data.get("project_id")
            quota_info = await fetch_geminicli_quota_info(
                access_token=access_token,
                project_id=project_id,
            )

        if quota_info.get("success"):
            # 自动同步 quota=0 的模型到 model_cooldowns
            try:
                import time
                from datetime import datetime as _dt
                models = quota_info.get("models", {}) or {}
                synced = []
                for model_name, info in models.items():
                    remaining = info.get("remaining")
                    if remaining is None or remaining > 0:
                        continue
                    # quota 为 0：尝试用 resetTimeRaw 解析 cooldown 时间
                    cooldown_until = None
                    raw = info.get("resetTimeRaw") or ""
                    if raw:
                        try:
                            iso = raw.replace("Z", "+00:00")
                            ts = _dt.fromisoformat(iso).timestamp()
                            # 1970-01-01（epoch 0）或已过期，用 4h 兜底
                            if ts < time.time() + 60:
                                ts = None
                            cooldown_until = ts
                        except Exception:
                            cooldown_until = None
                    if cooldown_until is None:
                        cooldown_until = time.time() + 4 * 3600

                    if hasattr(storage_adapter._backend, "set_model_cooldown"):
                        await storage_adapter._backend.set_model_cooldown(
                            filename, model_name, cooldown_until, mode=mode
                        )
                        synced.append(model_name)
                if synced:
                    log.info(f"[QUOTA SYNC] {filename}: 自动写入冷却的模型 {synced}")
            except Exception as sync_err:
                log.warning(f"[QUOTA SYNC] {filename}: 同步冷却失败: {sync_err}")

            return JSONResponse(content={
                "success": True,
                "filename": filename,
                "mode": mode,
                "models": quota_info.get("models", {})
            })
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "filename": filename,
                    "mode": mode,
                    "error": quota_info.get("error", "未知错误")
                }
            )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证额度失败 {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"获取额度失败: {str(e)}")


# ---------------------------------------------------------------------------
# 模型系列判断 / 额度检测 + 有额度自动解除冷却
# ---------------------------------------------------------------------------

def _model_family(model_name: str) -> Optional[str]:
    """根据模型名判断属于 Pro 还是 Flash 系列。

    返回 "pro" / "flash" / None。大小写不敏感。
    """
    if not model_name:
        return None
    name = model_name.lower()
    if "flash" in name:
        return "flash"
    if "pro" in name:
        return "pro"
    return None


async def _fetch_quota_for_credential(filename: str, mode: str) -> dict:
    """获取单个凭证的额度信息，不会主动同步 cooldown。

    返回结构:
        {
            "success": bool,
            "models": {model_name: {"remaining": float, ...}},
            "error": str,         # 可选
        }
    """
    storage_adapter = await get_storage_adapter()
    credential_data = await storage_adapter.get_credential(filename, mode=mode)
    if not credential_data:
        return {"success": False, "error": "凭证不存在"}

    creds = Credentials.from_dict(credential_data)
    await creds.refresh_if_needed()
    updated_data = creds.to_dict()
    if updated_data != credential_data:
        await storage_adapter.store_credential(filename, updated_data, mode=mode)
        credential_data = updated_data

    access_token = credential_data.get("access_token") or credential_data.get("token")
    if not access_token:
        return {"success": False, "error": "凭证中没有 access_token"}

    if mode == "antigravity":
        info = await fetch_quota_info(access_token)
    else:
        from src.api.geminicli import fetch_geminicli_quota_info
        project_id = credential_data.get("project_id")
        info = await fetch_geminicli_quota_info(
            access_token=access_token,
            project_id=project_id,
        )
    return info


@router.post("/batch-refresh-cooldown")
async def batch_refresh_cooldown(
    request: CredFileBatchTestRequest,
    mode: str = "geminicli",
    _token: str = Depends(verify_panel_token),
):
    """批量检测凭证额度，按模型实时 quota 双向同步 cooldown。

    双向逻辑（精确按模型名匹配）：
      1. 逐个凭证拉取 quota
      2. 解冷方向（清除误锁）：
         凭证 model_cooldowns 中未过期的记录里，对应模型实时 quota.remaining > 0
         → 解除冷却（capacity 抖动等误锁）
      3. 加冷方向（补漏锁）：
         实时 quota 里 remaining=0 的模型，如果当前没在冷却 / 冷却已过期
         → 写入冷却（quotaResetTimeStamp 优先，否则 4h 兜底）
      4. 不再使用 "系列" 粗粒度匹配，flash-lite 等独立 bucket 不会牵连普通 flash
      5. 返回汇总结果
    """
    try:
        mode = validate_mode(mode)
        filenames = [os.path.basename(name) for name in request.filenames if name]
        filenames = [name for name in filenames if name.endswith(".json")]

        if not filenames:
            raise HTTPException(status_code=400, detail="请选择要检测的凭证")

        storage_adapter = await get_storage_adapter()
        backend = getattr(storage_adapter, "_backend", None)
        can_set_cooldown = hasattr(backend, "set_model_cooldown")

        semaphore = asyncio.Semaphore(5)

        async def run_one(filename: str) -> dict:
            async with semaphore:
                try:
                    quota = await _fetch_quota_for_credential(filename, mode=mode)
                    if not quota.get("success"):
                        q_err = quota.get("error", "获取额度失败")
                        try:
                            await apply_probe_error_classification(filename, q_err, mode=mode)
                        except Exception as cls_err:
                            log.error(f"批量查额度错误分类失败 {filename}: {cls_err}")
                        return {
                            "filename": filename,
                            "success": False,
                            "error": q_err,
                            "licensable": is_license_error(q_err),
                        }
                    models = quota.get("models", {}) or {}

                    # 顺手统计一下系列状态，仅用于回显展示，不参与解冷决策
                    family_has_quota = {"pro": False, "flash": False}
                    for model_name, info in models.items():
                        family = _model_family(model_name)
                        if not family:
                            continue
                        remaining = info.get("remaining")
                        if remaining is not None and remaining > 0:
                            family_has_quota[family] = True

                    # 获取该凭证现有 cooldown
                    detail = await storage_adapter.get_credential_state(filename, mode=mode)
                    cooldowns = (detail or {}).get("model_cooldowns", {}) or {}

                    cleared = []           # 解冷：模型有额度但被锁
                    skipped_no_quota = []  # 不动：模型 quota=0 且已被锁，保持原样
                    skipped_unknown = []   # 不动：被锁的模型在实时 quota 里查不到
                    added_cooldown = []    # 加冷：模型 quota=0 但没在冷却，补冷
                    cooldown_skipped_active = []  # 不动：模型 quota=0 但已经在冷却中（无需重复）

                    now = time.time()
                    DEFAULT_COOLDOWN_HOURS = 4

                    def _resolve_cooldown_until_for_model(quota_entry: dict) -> float:
                        # 优先用 Google 给的 resetTimeRaw，必须是未来时间且非 epoch 0
                        from datetime import datetime as _dt
                        raw = quota_entry.get("resetTimeRaw") or ""
                        if raw:
                            try:
                                iso = raw.replace("Z", "+00:00")
                                ts = _dt.fromisoformat(iso).timestamp()
                                if ts > now + 60:
                                    return ts
                            except Exception:
                                pass
                        return now + DEFAULT_COOLDOWN_HOURS * 3600

                    if can_set_cooldown:
                        # === 解冷方向 ===
                        for cd_model, cd_until in list(cooldowns.items()):
                            try:
                                cd_ts = float(cd_until)
                            except (TypeError, ValueError):
                                continue
                            if cd_ts <= now:
                                continue  # 已过期不动

                            quota_entry = models.get(cd_model)
                            if quota_entry is None:
                                skipped_unknown.append(cd_model)
                                continue
                            remaining = quota_entry.get("remaining")
                            if remaining is None or remaining <= 0:
                                skipped_no_quota.append(cd_model)
                                continue

                            ok = await backend.set_model_cooldown(
                                filename, cd_model, None, mode=mode
                            )
                            if ok:
                                cleared.append(cd_model)

                        # === 加冷方向 ===
                        for model_name, info in models.items():
                            remaining = info.get("remaining")
                            if remaining is None or remaining > 0:
                                continue  # 有额度，不补冷

                            # quota=0 的模型
                            existing = cooldowns.get(model_name)
                            existing_ts = None
                            if existing is not None:
                                try:
                                    existing_ts = float(existing)
                                except (TypeError, ValueError):
                                    existing_ts = None
                            # 已经在冷却中（未过期）就不重复设置
                            if existing_ts is not None and existing_ts > now:
                                cooldown_skipped_active.append(model_name)
                                continue

                            cooldown_until = _resolve_cooldown_until_for_model(info)
                            ok = await backend.set_model_cooldown(
                                filename, model_name, cooldown_until, mode=mode
                            )
                            if ok:
                                added_cooldown.append(model_name)

                    return {
                        "filename": filename,
                        "success": True,
                        "family_has_quota": family_has_quota,
                        "cleared": cleared,
                        "skipped_no_quota": skipped_no_quota,
                        "skipped_unknown": skipped_unknown,
                        "added_cooldown": added_cooldown,
                        "cooldown_skipped_active": cooldown_skipped_active,
                        "model_count": len(models),
                    }
                except Exception as e:
                    log.warning(f"[BATCH REFRESH COOLDOWN] {filename} 失败: {e}")
                    return {
                        "filename": filename,
                        "success": False,
                        "error": str(e),
                    }

        results = await asyncio.gather(*(run_one(filename) for filename in filenames))
        success_count = sum(1 for r in results if r.get("success"))
        cleared_total = sum(len(r.get("cleared", [])) for r in results)
        added_total = sum(len(r.get("added_cooldown", [])) for r in results)
        affected_creds_cleared = sum(1 for r in results if r.get("cleared"))
        affected_creds_added = sum(1 for r in results if r.get("added_cooldown"))

        return JSONResponse(content={
            "success_count": success_count,
            "failure_count": len(results) - success_count,
            "total_count": len(results),
            "cleared_total": cleared_total,
            "added_total": added_total,
            "affected_creds": affected_creds_cleared,           # 兼容旧字段
            "affected_creds_cleared": affected_creds_cleared,
            "affected_creds_added": affected_creds_added,
            "results": results,
            "message": (
                f"完成：{success_count}/{len(results)} 凭证拉取额度成功，"
                f"解除 {cleared_total} 个冷却（涉及 {affected_creds_cleared} 凭证），"
                f"补加 {added_total} 个冷却（涉及 {affected_creds_added} 凭证）"
            ),
        })

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量检测额度失败: {e}")
        raise HTTPException(status_code=500, detail=f"批量检测额度失败: {str(e)}")


@router.post("/configure-preview/{filename}")
async def configure_preview_channel(
    filename: str,
    token: str = Depends(verify_panel_token),
    mode: str = "geminicli"
):
    """
    为 geminicli 凭证配置 preview 通道

    通过调用 Google Cloud API 设置 release_channel 为 EXPERIMENTAL

    Args:
        filename: 凭证文件名
        mode: 凭证模式（仅支持 geminicli）

    Returns:
        配置结果信息
    """
    try:
        mode = validate_mode(mode)

        # 只支持 geminicli 模式
        if mode != "geminicli":
            raise HTTPException(
                status_code=400,
                detail="配置 preview 通道仅支持 geminicli 模式"
            )

        # 验证文件名
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")

        storage_adapter = await get_storage_adapter()

        # 获取凭证数据
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        # 创建凭证对象并刷新 token（如果需要）
        credentials = Credentials.from_dict(credential_data)
        token_refreshed = await credentials.refresh_if_needed()

        if token_refreshed:
            log.info(f"Token已自动刷新: {filename}")
            credential_data = credentials.to_dict()
            await storage_adapter.store_credential(filename, credential_data, mode=mode)

        # 获取 access_token 和 project_id
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")

        if not access_token:
            raise HTTPException(status_code=400, detail="凭证中没有访问令牌")
        if not project_id:
            raise HTTPException(status_code=400, detail="凭证中没有项目ID")

        # 调用 Google Cloud API 配置 preview 通道
        # 根据文档，需要两个步骤：
        # 1. 创建 Release Channel Setting (EXPERIMENTAL)
        # 2. 创建 Setting Binding (绑定到目标项目)
        from src.httpx_client import post_async
        import uuid

        # 生成唯一的 ID
        setting_id = f"preview-setting-{uuid.uuid4().hex[:8]}"
        binding_id = f"preview-binding-{uuid.uuid4().hex[:8]}"

        base_url = f"https://cloudaicompanion.googleapis.com/v1/projects/{project_id}/locations/global"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        log.info(f"开始配置 preview 通道: {filename} (project_id={project_id})")

        # 步骤 1: 创建 Release Channel Setting
        setting_url = f"{base_url}/releaseChannelSettings"
        setting_response = await post_async(
            url=setting_url,
            json={"release_channel": "EXPERIMENTAL"},
            headers=headers,
            params={"release_channel_setting_id": setting_id},
            timeout=30.0
        )

        setting_status = setting_response.status_code

        # 调用 Google Cloud API 配置 preview 通道
        # 根据文档，需要两个步骤：
        # 1. 创建 Release Channel Setting (EXPERIMENTAL)
        # 2. 创建 Setting Binding (绑定到目标项目)
        from src.httpx_client import post_async, get_async
        import uuid

        # 生成唯一的 ID
        setting_id = f"preview-setting-{uuid.uuid4().hex[:8]}"
        binding_id = f"preview-binding-{uuid.uuid4().hex[:8]}"

        base_url = f"https://cloudaicompanion.googleapis.com/v1/projects/{project_id}/locations/global"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        log.info(f"开始配置 preview 通道: {filename} (project_id={project_id})")

        # 步骤 1: 创建 Release Channel Setting
        setting_url = f"{base_url}/releaseChannelSettings"
        setting_response = await post_async(
            url=setting_url,
            json={"release_channel": "EXPERIMENTAL"},
            headers=headers,
            params={"release_channel_setting_id": setting_id},
            timeout=30.0
        )

        setting_status = setting_response.status_code

        if setting_status == 200 or setting_status == 201:
            log.info(f"步骤 1/2: Release Channel Setting 创建成功 (setting_id={setting_id})")
        elif setting_status == 409:
            # Setting 已存在，需要 LIST 获取真实的 setting_id，否则 Step 2 的 URL 会用错误的 ID
            log.info(f"步骤 1/2: Release Channel Setting 已存在，正在获取已有 setting_id...")
            list_response = await get_async(
                url=setting_url,
                headers=headers,
                timeout=30.0
            )
            if list_response.status_code == 200:
                try:
                    list_data = list_response.json()
                    settings = list_data.get("releaseChannelSettings", [])
                    if settings:
                        existing_name = settings[0].get("name", "")
                        setting_id = existing_name.split("/")[-1]
                        log.info(f"步骤 1/2: 获取到已有 setting_id={setting_id}")
                    else:
                        log.warning(f"步骤 1/2: LIST 返回空列表，保持随机 setting_id")
                except Exception as e:
                    log.warning(f"步骤 1/2: 解析 LIST 响应失败: {e}，保持随机 setting_id")
            else:
                log.warning(f"步骤 1/2: LIST 请求失败 (status={list_response.status_code})，保持随机 setting_id")
        else:
            # 步骤 1 失败
            error_text = setting_response.text if hasattr(setting_response, 'text') else ""
            log.error(f"步骤 1/2 失败: {filename} - Status: {setting_status}, Error: {error_text}")

            return JSONResponse(
                status_code=setting_status,
                content={
                    "success": False,
                    "filename": filename,
                    "preview": False,
                    "message": f"创建 Release Channel Setting 失败: HTTP {setting_status}",
                    "error": error_text,
                    "step": "create_setting"
                }
            )

        # 步骤 2: 创建 Setting Binding (绑定到当前项目)
        binding_url = f"{base_url}/releaseChannelSettings/{setting_id}/settingBindings"
        binding_response = await post_async(
            url=binding_url,
            json={
                "target": f"projects/{project_id}",
                "product": "GEMINI_CODE_ASSIST"
            },
            headers=headers,
            params={"setting_binding_id": binding_id},
            timeout=30.0
        )

        binding_status = binding_response.status_code

        if binding_status == 200 or binding_status == 201:
            await storage_adapter.update_credential_state(filename, {
                "preview": True
            }, mode=mode)

            log.info(f"步骤 2/2: Setting Binding 创建成功 - Preview 通道配置完成: {filename}")

            return JSONResponse(content={
                "success": True,
                "filename": filename,
                "preview": True,
                "message": "Preview 通道配置成功，已将 preview 属性设置为 true",
                "setting_id": setting_id,
                "binding_id": binding_id
            })
        elif binding_status == 409:
            # Binding 已存在，说明已经配置过了
            await storage_adapter.update_credential_state(filename, {
                "preview": True
            }, mode=mode)

            log.info(f"步骤 2/2: Setting Binding 已存在 - Preview 通道已配置: {filename}")

            return JSONResponse(content={
                "success": True,
                "filename": filename,
                "preview": True,
                "message": "Preview 通道配置已存在，已将 preview 属性设置为 true"
            })
        else:
            # 步骤 2 失败
            error_text = binding_response.text if hasattr(binding_response, 'text') else ""
            log.error(f"步骤 2/2 失败: {filename} - Status: {binding_status}, Error: {error_text}")

            return JSONResponse(
                status_code=binding_status,
                content={
                    "success": False,
                    "filename": filename,
                    "preview": False,
                    "message": f"创建 Setting Binding 失败: HTTP {binding_status}",
                    "error": error_text,
                    "step": "create_binding"
                }
            )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"配置 preview 通道失败 {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"配置失败: {str(e)}")


async def test_credential_common(filename: str, mode: str = "geminicli") -> JSONResponse:
    """
    测试指定凭证是否可用

    Args:
        filename: 凭证文件名
        mode: 凭证模式（geminicli 或 antigravity）

    Returns:
        返回状态码：
        - 200: 凭证可用
        - 429: 凭证被限流但有效
        - 其他: 凭证失败（返回实际错误码）
    """
    try:
        mode = validate_mode(mode)

        # 验证文件名
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")

        storage_adapter = await get_storage_adapter()

        # 获取凭证数据
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        # 创建凭证对象并尝试刷新 token（如果需要）
        credentials = Credentials.from_dict(credential_data)
        token_refreshed = await credentials.refresh_if_needed()

        # 如果 token 被刷新了，更新存储
        if token_refreshed:
            log.info(f"Token已自动刷新: {filename} (mode={mode})")
            credential_data = credentials.to_dict()
            await storage_adapter.store_credential(filename, credential_data, mode=mode)

        # 获取访问令牌
        access_token = credential_data.get("access_token") or credential_data.get("token")
        if not access_token:
            raise HTTPException(status_code=400, detail="凭证中没有访问令牌")

        # 根据模式构造测试请求
        from src.httpx_client import post_async

        # 获取 project_id
        project_id = credential_data.get("project_id", "")
        if not project_id:
            raise HTTPException(status_code=400, detail="凭证中没有项目ID")

        # 根据模式选择 API 端点和请求头
        # 对于 geminicli 模式，使用两次测试：gemini-2.5-flash 和 gemini-3-flash-preview
        # 对于 antigravity 模式，只使用 gemini-2.5-flash
        test_model = "gemini-2.5-flash"

        if mode == "antigravity":
            api_base_url = await get_antigravity_api_url()
            from src.api.antigravity import build_antigravity_headers
            headers = build_antigravity_headers(access_token)
        else:
            api_base_url = await get_code_assist_endpoint()
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "User-Agent": GEMINICLI_USER_AGENT,
            }

        # 第一次测试：使用 gemini-2.5-flash
        response = await post_async(
            url=f"{api_base_url}/v1internal:generateContent",
            json={
                "model": test_model,
                "project": project_id,
                "request": {
                    "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
                    "generationConfig": {"maxOutputTokens": 1}
                }
            },
            headers=headers,
            timeout=30.0
        )

        # 返回实际的状态码和详细信息
        status_code = response.status_code

        if status_code == 200 or status_code == 429:
            log.info(f"凭证测试成功: {filename} (mode={mode}, model={test_model}, status={status_code})")
            # 测试成功时清除错误状态
            if status_code == 200:
                if hasattr(storage_adapter._backend, "record_success"):
                    await storage_adapter._backend.record_success(
                        filename,
                        model_name=test_model,
                        mode=mode,
                    )
                else:
                    await storage_adapter.update_credential_state(filename, {
                        "error_codes": [],
                        "error_messages": {}
                    }, mode=mode)

                # 如果是 geminicli 模式且第一次测试成功，继续测试 gemini-3-flash-preview
                if mode == "geminicli":
                    preview_model = "gemini-3-flash-preview"
                    log.info(f"开始测试 preview 模型: {filename} (model={preview_model})")

                    try:
                        preview_response = await post_async(
                            url=f"{api_base_url}/v1internal:generateContent",
                            json={
                                "model": preview_model,
                                "project": project_id,
                                "request": {
                                    "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
                                    "generationConfig": {"maxOutputTokens": 1}
                                }
                            },
                            headers=headers,
                            timeout=30.0
                        )

                        preview_status = preview_response.status_code

                        if preview_status == 200 or preview_status == 429:
                            # preview 模型测试成功，设置 preview=True
                            log.info(f"Preview 模型测试成功: {filename} (status={preview_status})")
                            await storage_adapter.update_credential_state(filename, {
                                "preview": True
                            }, mode=mode)
                        elif preview_status == 404:
                            # preview 模型返回 404，说明不支持，设置 preview=False
                            log.warning(f"Preview 模型不支持: {filename} (status=404)")
                            await storage_adapter.update_credential_state(filename, {
                                "preview": False
                            }, mode=mode)
                        else:
                            # 其他错误，保持默认 preview 状态
                            log.warning(f"Preview 模型测试失败: {filename} (status={preview_status})")
                    except Exception as e:
                        log.error(f"Preview 模型测试异常: {filename} - {e}")
            else:
                error_text = response.text if hasattr(response, 'text') else ""
                if hasattr(storage_adapter._backend, "record_failure"):
                    await storage_adapter._backend.record_failure(
                        filename,
                        status_code,
                        error_message=error_text,
                        mode=mode,
                    )

            # 返回成功响应
            return JSONResponse(
                status_code=status_code,
                content={
                    "success": True,
                    "status_code": status_code,
                    "message": "测试成功",
                    "filename": filename
                }
            )
        else:
            log.warning(f"凭证测试失败: {filename} (mode={mode}, status={status_code})")
            # 测试失败时保存错误码和错误消息（覆盖模式，只保存最新的一个错误）
            try:
                error_text = response.text if hasattr(response, 'text') else ""

                # 打印详细错误内容到日志
                log.error(f"凭证测试错误详情 - 文件: {filename}, 模式: {mode}, 状态码: {status_code}, 错误内容: {error_text}")

                if hasattr(storage_adapter._backend, "record_failure"):
                    await storage_adapter._backend.record_failure(
                        filename,
                        status_code,
                        error_message=error_text,
                        mode=mode,
                    )
                else:
                    # 使用覆盖模式保存错误（与 credential_manager 保持一致）
                    error_codes = [status_code]
                    error_messages = {str(status_code): error_text if error_text else f"HTTP {status_code}"}

                    # 更新状态
                    await storage_adapter.update_credential_state(filename, {
                        "error_codes": error_codes,
                        "error_messages": error_messages
                    }, mode=mode)

                log.info(f"已保存测试错误信息: {filename} - 错误码 {status_code}")

                # 测试失败分流：license 报错 → 「可授权」（禁用不参与调用，可批量启用恢复）；
                # 其他错误（含 429 quota exhausted）只打错误码；auto_ban 名单内的再走自动封禁
                try:
                    if is_license_error(error_text):
                        log.warning(
                            f"[BATCH-TEST LICENSABLE] license error on {filename} "
                            f"(mode={mode}, status={status_code}) -> 可授权"
                        )
                        await credential_manager.set_cred_licensable(filename, True, mode=mode)
                    else:
                        if await check_should_auto_ban(status_code):
                            log.warning(
                                f"[BATCH-TEST AUTO_BAN] Status {status_code} triggers auto-ban "
                                f"for credential: {filename} (mode={mode})"
                            )
                            await credential_manager.set_cred_disabled(
                                filename, True, mode=mode
                            )
                except Exception as ban_err:
                    log.error(f"测试失败分类/封禁处理异常 {filename}: {ban_err}")
            except Exception as e:
                log.error(f"保存测试错误信息失败: {e}")

        # 返回错误响应，包含完整的错误信息
        error_text = response.text if hasattr(response, 'text') else ""

        return JSONResponse(
            status_code=status_code,
            content={
                "success": False,
                "status_code": status_code,
                "message": f"测试失败: HTTP {status_code}",
                "error": error_text,
                "filename": filename
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"测试凭证失败 {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"测试失败: {str(e)}")



async def test_credential_model_common(
    filename: str,
    model: str,
    mode: str = "geminicli",
) -> JSONResponse:
    """测试指定凭证 + 指定模型是否可用（不改 preview 状态，不触发二次 preview 探测）。"""
    try:
        mode = validate_mode(mode)
        model = (model or "").strip()
        if not model:
            raise HTTPException(status_code=400, detail="model 不能为空")
        if not filename.endswith(".json"):
            raise HTTPException(status_code=400, detail="无效的文件名")

        storage_adapter = await get_storage_adapter()
        credential_data = await storage_adapter.get_credential(filename, mode=mode)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        credentials = Credentials.from_dict(credential_data)
        token_refreshed = await credentials.refresh_if_needed()
        if token_refreshed:
            log.info(f"Token已自动刷新: {filename} (mode={mode}, model-test={model})")
            credential_data = credentials.to_dict()
            await storage_adapter.store_credential(filename, credential_data, mode=mode)

        access_token = credential_data.get("access_token") or credential_data.get("token")
        if not access_token:
            raise HTTPException(status_code=400, detail="凭证中没有访问令牌")

        project_id = credential_data.get("project_id", "")
        if not project_id and mode == "geminicli":
            raise HTTPException(status_code=400, detail="凭证中没有项目ID")

        if mode == "antigravity":
            api_base_url = await get_antigravity_api_url()
            from src.api.antigravity import build_antigravity_headers
            headers = build_antigravity_headers(access_token)
        else:
            api_base_url = await get_code_assist_endpoint()
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "User-Agent": GEMINICLI_USER_AGENT,
            }

        payload = {
            "model": model,
            "project": project_id or "",
            "request": {
                "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
                "generationConfig": {"maxOutputTokens": 1},
            },
        }

        response = await post_async(
            url=f"{api_base_url}/v1internal:generateContent",
            json=payload,
            headers=headers,
            timeout=30.0,
        )
        status_code = response.status_code
        error_text = response.text if hasattr(response, "text") else ""

        if status_code == 200 or status_code == 429:
            log.info(
                f"模型测试成功: {filename} (mode={mode}, model={model}, status={status_code})"
            )
            if status_code == 200 and hasattr(storage_adapter._backend, "record_success"):
                try:
                    await storage_adapter._backend.record_success(
                        filename,
                        model_name=model,
                        mode=mode,
                    )
                except Exception as e:
                    log.warning(f"record_success 失败: {e}")
            return JSONResponse(
                status_code=status_code,
                content={
                    "success": True,
                    "status_code": status_code,
                    "message": "测试成功" if status_code == 200 else "限流但仍有效(429)",
                    "filename": filename,
                    "model": model,
                    "mode": mode,
                },
            )

        log.warning(
            f"模型测试失败: {filename} (mode={mode}, model={model}, status={status_code})"
        )
        log.error(
            f"模型测试错误详情 - 文件: {filename}, 模式: {mode}, 模型: {model}, "
            f"状态码: {status_code}, 错误内容: {error_text}"
        )
        return JSONResponse(
            status_code=status_code if status_code >= 400 else 400,
            content={
                "success": False,
                "status_code": status_code,
                "message": f"测试失败: HTTP {status_code}",
                "error": error_text,
                "filename": filename,
                "model": model,
                "mode": mode,
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"测试模型失败 {filename}/{model}: {e}")
        raise HTTPException(status_code=500, detail=f"测试失败: {str(e)}")


@router.post("/test-model/{filename}")
async def test_credential_model(
    filename: str,
    model: str,
    mode: str = "geminicli",
    _token: str = Depends(verify_panel_token),
):
    """测试指定凭证的指定模型。model 通过 query 参数传入。"""
    return await test_credential_model_common(filename, model=model, mode=mode)


@router.post("/test/{filename}")
async def test_credential(
    filename: str,
    mode: str = "geminicli",
    _token: str = Depends(verify_panel_token)
):
    return await test_credential_common(filename, mode=mode)


@router.post("/batch-test")
async def batch_test_credentials(
    request: CredFileBatchTestRequest,
    mode: str = "geminicli",
    _token: str = Depends(verify_panel_token)
):
    """批量测试选中的凭证。"""
    try:
        mode = validate_mode(mode)
        filenames = [os.path.basename(name) for name in request.filenames if name]
        filenames = [name for name in filenames if name.endswith(".json")]

        if not filenames:
            raise HTTPException(status_code=400, detail="请选择要测试的凭证")

        semaphore = asyncio.Semaphore(5)

        async def run_one(filename: str) -> dict:
            async with semaphore:
                try:
                    response = await test_credential_common(filename, mode=mode)
                    body = json.loads(response.body.decode("utf-8"))
                    ok = response.status_code == 200 and body.get("success", False)
                    err = body.get("error")
                    return {
                        "filename": filename,
                        "success": ok,
                        "status_code": body.get("status_code", response.status_code),
                        "message": body.get("message") or ("测试成功" if ok else "测试失败"),
                        "error": err,
                        "licensable": bool(not ok and is_license_error(err)),
                    }
                except HTTPException as e:
                    return {
                        "filename": filename,
                        "success": False,
                        "status_code": e.status_code,
                        "message": str(e.detail),
                    }
                except Exception as e:
                    log.error(f"批量测试凭证失败 {filename}: {e}")
                    return {
                        "filename": filename,
                        "success": False,
                        "status_code": 500,
                        "message": str(e),
                    }

        results = await asyncio.gather(*(run_one(filename) for filename in filenames))
        success_count = sum(1 for item in results if item.get("success"))
        failure_count = len(results) - success_count

        return JSONResponse(content={
            "success_count": success_count,
            "failure_count": failure_count,
            "total_count": len(results),
            "results": results,
            "message": f"批量消息测试完成：成功 {success_count}/{len(results)} 个凭证",
        })

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量测试凭证失败: {e}")
        raise HTTPException(status_code=500, detail=f"批量测试失败: {str(e)}")


# =============================================================================
# Refresh Token 一键添加凭证
# =============================================================================

# Google Gemini CLI 官方默认 OAuth Client（公开值，从 gemini-cli 源码可查）
DEFAULT_GEMINI_CLI_CLIENT_ID = "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com"
DEFAULT_GEMINI_CLI_CLIENT_SECRET = "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"


async def _exchange_refresh_token_to_credential(
    refresh_token: str,
    client_id: str,
    client_secret: str,
) -> dict:
    """用 refresh_token 换取完整凭证字段，返回 credential dict"""
    oauth_base_url = await get_oauth_proxy_url()
    token_url = f"{oauth_base_url.rstrip('/')}/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    response = await post_async(
        token_url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    response.raise_for_status()
    token_data = response.json()

    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError("响应中未返回 access_token")

    expires_in = int(token_data.get("expires_in", 3600))
    expiry = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    # refresh_token 可能在响应中刷新返回新值
    new_refresh = token_data.get("refresh_token") or refresh_token

    return {
        "access_token": access_token,
        "token": access_token,  # 兼容字段
        "refresh_token": new_refresh,
        "client_id": client_id,
        "client_secret": client_secret,
        "scopes": token_data.get("scope", "").split() if token_data.get("scope") else None,
        "expiry": expiry,
        "token_uri": f"{oauth_base_url.rstrip('/')}/token",
    }


@router.post("/upload-by-refresh-token")
async def upload_credentials_by_refresh_token(
    req: RefreshTokenAddRequest,
    token: str = Depends(verify_panel_token),
):
    """通过 refresh_token 一键添加凭证

    流程：
    1. 用 refresh_token 调 OAuth /token 端点换 access_token
    2. 如果未提供 project_id，自动调 loadCodeAssist/onboardUser 探测
    3. 组装成完整凭证 JSON 并入库
    """
    try:
        mode = validate_mode(req.mode or "geminicli")

        if not req.refresh_token or not req.refresh_token.strip():
            raise HTTPException(status_code=400, detail="refresh_token 不能为空")

        result = await _add_credential_by_refresh_token(
            refresh_token=req.refresh_token.strip(),
            client_id=req.client_id,
            client_secret=req.client_secret,
            project_id=req.project_id,
            custom_filename=req.custom_filename,
            mode=mode,
        )

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])

        return JSONResponse(content={
            "success": True,
            "filename": result["filename"],
            "project_id": result["project_id"],
            "subscription_tier": result["subscription_tier"],
            "mode": mode,
            "message": f"凭证添加成功: {result['filename']}",
        })

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"通过 refresh_token 添加凭证失败: {e}")
        raise HTTPException(status_code=500, detail=f"添加失败: {str(e)}")


@router.post("/upload-by-refresh-token-batch")
async def upload_credentials_by_refresh_token_batch(
    req: RefreshTokenBatchAddRequest,
    token: str = Depends(verify_panel_token),
):
    """批量通过 refresh_token 添加凭证。

    并发执行，限流 5，避免多个 token 同时冲击 Google API。
    """
    try:
        mode = validate_mode(req.mode or "geminicli")

        # 去重 + 去空行
        seen = set()
        tokens: List[str] = []
        for raw in req.refresh_tokens or []:
            t = (raw or "").strip()
            if not t or t in seen:
                continue
            seen.add(t)
            tokens.append(t)

        if not tokens:
            raise HTTPException(status_code=400, detail="未提供有效的 refresh_token")

        if len(tokens) > 200:
            raise HTTPException(status_code=400, detail=f"批量数量过多，最多 200 个，当前 {len(tokens)} 个")

        prefix = (req.filename_prefix or "").strip() or None

        semaphore = asyncio.Semaphore(5)

        async def run_one(idx: int, rt: str) -> dict:
            async with semaphore:
                custom = f"{prefix}-{idx+1}" if prefix else None
                try:
                    result = await _add_credential_by_refresh_token(
                        refresh_token=rt,
                        client_id=req.client_id,
                        client_secret=req.client_secret,
                        project_id=None,  # 批量场景不手填 project_id
                        custom_filename=custom,
                        mode=mode,
                    )
                    return {
                        "index": idx,
                        "refresh_token_preview": rt[:12] + "..." + rt[-6:] if len(rt) > 24 else rt,
                        "success": result["success"],
                        "filename": result.get("filename"),
                        "project_id": result.get("project_id"),
                        "subscription_tier": result.get("subscription_tier"),
                        "error": result.get("error"),
                    }
                except Exception as e:
                    log.error(f"批量添加第 {idx+1} 个 refresh_token 失败: {e}")
                    return {
                        "index": idx,
                        "refresh_token_preview": rt[:12] + "..." + rt[-6:] if len(rt) > 24 else rt,
                        "success": False,
                        "error": str(e),
                    }

        results = await asyncio.gather(*(run_one(i, t) for i, t in enumerate(tokens)))
        success_count = sum(1 for r in results if r.get("success"))
        failure_count = len(results) - success_count

        return JSONResponse(content={
            "success_count": success_count,
            "failure_count": failure_count,
            "total_count": len(results),
            "results": results,
            "message": f"批量添加完成：成功 {success_count}/{len(results)}",
        })

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量通过 refresh_token 添加凭证失败: {e}")
        raise HTTPException(status_code=500, detail=f"批量添加失败: {str(e)}")


async def _add_credential_by_refresh_token(
    refresh_token: str,
    client_id: Optional[str],
    client_secret: Optional[str],
    project_id: Optional[str],
    custom_filename: Optional[str],
    mode: str,
) -> dict:
    """核心逻辑：换 token + 探测 project + 入库。单个/批量接口共用。"""
    # refresh_token 绑定签发它的 OAuth client：
    # - antigravity 必须用 ANTIGRAVITY_CLIENT_*
    # - geminicli 用 Gemini CLI 官方 client
    # 用户显式传入 client_id/secret 时优先生效
    if mode == "antigravity":
        default_cid = ANTIGRAVITY_CLIENT_ID
        default_csec = ANTIGRAVITY_CLIENT_SECRET
    else:
        default_cid = DEFAULT_GEMINI_CLI_CLIENT_ID or UTILS_GEMINI_CLIENT_ID
        default_csec = DEFAULT_GEMINI_CLI_CLIENT_SECRET or UTILS_GEMINI_CLIENT_SECRET
    cid = (client_id or default_cid).strip()
    csec = (client_secret or default_csec).strip()

    # 1. 换 access_token
    try:
        credential_data = await _exchange_refresh_token_to_credential(
            refresh_token=refresh_token,
            client_id=cid,
            client_secret=csec,
        )
    except Exception as e:
        log.error(f"refresh_token 换 access_token 失败: {e}")
        return {
            "success": False,
            "error": f"refresh_token 无效或网络异常: {e}",
        }

    # 2. 准备并验收 project_id（失败凭证隔离保存，不进入可用池）
    pid = (project_id or "").strip() or None
    subscription_tier = None

    if mode == "geminicli":
        try:
            credentials = Credentials.from_dict(credential_data)
            pid = pid or await ensure_geminicli_project(credentials)
            if not await enable_required_apis(credentials, pid):
                raise RuntimeError(f"项目 {pid} 的 Gemini API 启用失败")
            for attempt in range(1, 9):
                if await validate_geminicli_project(credentials, pid):
                    break
                if attempt == 8:
                    raise RuntimeError(f"项目 {pid} 的 GeminiCLI 真实验收失败")
                await asyncio.sleep(5)
        except Exception as e:
            log.warning(f"GeminiCLI 凭证暂未通过验收，将隔离后后台重试: {e}")
            pid = None
    elif not pid:
        pid, subscription_tier = await _detect_project_id_once(credential_data, mode)

    if pid:
        credential_data["project_id"] = pid

    # 3. 生成文件名：禁止用 project_id 当文件名，否则同项目多 token 会互相覆盖
    filename = _build_unique_refresh_filename(refresh_token, custom_filename)

    # 4. 入库
    if mode == "antigravity":
        await credential_manager.add_antigravity_credential(filename, credential_data)
    else:
        await credential_manager.add_credential(filename, credential_data)

    project_id_pending = not bool(pid)
    if project_id_pending:
        # 先从轮询池隔离；后台真实验收成功后才重新启用。
        await credential_manager.update_credential_state(
            filename, {"disabled": True}, mode=mode
        )
        create_managed_task(
            _retry_project_id_in_background(filename, mode, max_attempts=10),
            name=f"retry-project-id:{filename}",
        )
        log.info(f"通过 refresh_token 成功添加凭证: {filename} (mode={mode}, project_id 待后台重试)")
    else:
        log.info(f"通过 refresh_token 成功添加凭证: {filename} (mode={mode}, project_id={pid})")

    return {
        "success": True,
        "filename": filename,
        "project_id": pid,
        "subscription_tier": subscription_tier,
        "project_id_pending": project_id_pending,
    }


def _build_unique_refresh_filename(refresh_token: str, custom_filename: Optional[str]) -> str:
    """每条 refresh_token 必须有独立文件名，不能用 GCP project_id。"""
    if custom_filename:
        base = os.path.basename(custom_filename.strip())
        if not base.endswith(".json"):
            base += ".json"
        return base
    digest = hashlib.sha1((refresh_token or "").encode("utf-8")).hexdigest()[:10]
    return f"refresh-{time.time_ns()}-{digest}.json"


async def _detect_project_id_once(
    credential_data: dict,
    mode: str,
) -> Tuple[Optional[str], Optional[str]]:
    """探测一次 project_id / subscription_tier。失败返回 (None, None)。"""
    try:
        if mode == "geminicli":
            credentials = Credentials.from_dict(credential_data)
            project_id = await ensure_geminicli_project(credentials)
            enabled = await enable_required_apis(credentials, project_id)
            if not enabled:
                raise RuntimeError(f"项目 {project_id} 的 Gemini API 启用失败")
            # 新启用服务可能需要短暂传播；只在真实模型请求成功后写回入池。
            for attempt in range(1, 9):
                if await validate_geminicli_project(credentials, project_id):
                    break
                if attempt == 8:
                    raise RuntimeError(f"项目 {project_id} 的 GeminiCLI 真实验收失败")
                await asyncio.sleep(5)
            return project_id, None

        api_base_url = await get_antigravity_api_url()
        detected = await fetch_project_id_and_tier(
            access_token=credential_data["access_token"],
            user_agent=ANTIGRAVITY_USER_AGENT,
            api_base_url=api_base_url,
        )
        if not detected:
            return None, None
        pid = detected[0]
        tier = detected[1] if len(detected) > 1 else None
        return pid, tier
    except Exception as e:
        log.warning(f"自动探测 project_id 失败: {e}")
        return None, None


async def _retry_project_id_in_background(filename: str, mode: str, max_attempts: int = 10) -> None:
    """导入时没探到 project_id，后台最多再试 max_attempts 次，成功则回写同一条凭证。"""
    for attempt in range(1, max_attempts + 1):
        await asyncio.sleep(2 if attempt == 1 else min(8, attempt + 1))
        try:
            storage = await get_storage_adapter()
            current = await storage.get_credential(filename, mode=mode)
            if not current:
                log.warning(f"后台补探测中止：凭证已不存在 {filename}")
                return
            if (current.get("project_id") or "").strip():
                log.info(f"后台补探测跳过：{filename} 已有 project_id")
                return

            pid, _tier = await _detect_project_id_once(current, mode)
            if not pid:
                log.info(f"后台补探测 project_id 第 {attempt}/{max_attempts} 次未成功: {filename}")
                continue

            latest = await storage.get_credential(filename, mode=mode)
            if not latest:
                log.warning(f"后台补探测中止：回写前凭证已不存在 {filename}")
                return
            latest["project_id"] = pid
            await storage.store_credential(filename, latest, mode=mode)
            await storage.update_credential_state(
                filename, {"disabled": False}, mode=mode
            )
            log.info(f"后台补探测 project_id 成功: {filename} -> {pid} (第 {attempt}/{max_attempts} 次)")
            return
        except Exception as e:
            log.warning(f"后台补探测 project_id 第 {attempt}/{max_attempts} 次异常 {filename}: {e}")
    log.warning(f"后台补探测 project_id 已重试 {max_attempts} 次仍失败: {filename}")


# =============================================================================
# 延迟补位统计
# =============================================================================

@router.get("/hedge-stats")
async def get_delayed_hedge_stats(
    token: str = Depends(verify_panel_token),
    mode: Optional[str] = None,
):
    if mode:
        mode = validate_mode(mode)
    storage_adapter = await get_storage_adapter()
    backend = getattr(storage_adapter, "_backend", None)
    empty = {
        "triggered": 0,
        "upstream_requests": 0,
        "extra_requests": 0,
        "primary_won": 0,
        "backup_won": 0,
        "rescued": 0,
        "by_mode": {},
    }
    if backend is None or not hasattr(backend, "get_hedge_stats"):
        return JSONResponse(content={**empty, "note": "当前存储后端不支持补位统计"})
    return JSONResponse(content=await backend.get_hedge_stats(mode=mode))


# =============================================================================
# 每日调用统计
# =============================================================================

@router.get("/stats-today")
async def get_today_stats(
    token: str = Depends(verify_panel_token),
    mode: Optional[str] = None,
):
    """今天（北京时间）的总调用统计。

    Query 可选 ?mode=geminicli|antigravity，不传则返回总和+按模式拆分。
    """
    try:
        if mode:
            mode = validate_mode(mode)
        storage_adapter = await get_storage_adapter()
        backend = getattr(storage_adapter, "_backend", None)
        if backend is None or not hasattr(backend, "get_today_stats"):
            return JSONResponse(content={
                "date": "",
                "success_count": 0,
                "failure_count": 0,
                "total_count": 0,
                "note": "当前存储后端不支持按日统计",
            })
        data = await backend.get_today_stats(mode=mode)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取今日统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats-recent")
async def get_recent_daily_stats(
    token: str = Depends(verify_panel_token),
    days: int = 7,
    mode: Optional[str] = None,
):
    """最近 N 天的每日调用统计（默认 7，最大 90）。"""
    try:
        if mode:
            mode = validate_mode(mode)
        storage_adapter = await get_storage_adapter()
        backend = getattr(storage_adapter, "_backend", None)
        if backend is None or not hasattr(backend, "get_recent_daily_stats"):
            return JSONResponse(content={"days": days, "items": []})
        items = await backend.get_recent_daily_stats(days=days, mode=mode)
        return JSONResponse(content={"days": days, "items": items})
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取近期统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats-today-by-model")
async def get_today_stats_by_model(
    token: str = Depends(verify_panel_token),
    mode: Optional[str] = None,
):
    """今天按模型家族汇总的统计 + 当前 RPM。

    模型家族归一化规则：
    - gemini-2.5-pro / gemini-2.5-pro-search / gemini-2.5-pro-thinking → 2.5-pro
    - gemini-2.5-flash / gemini-2.5-flash-lite → 2.5-flash / 2.5-flash-lite
    - gemini-3-pro-preview / gemini-3.1-pro-preview / gemini-3-flash-preview / gemini-3.1-flash-lite-preview
    - 其他归入 other
    """
    try:
        if mode:
            mode = validate_mode(mode)
        storage_adapter = await get_storage_adapter()
        backend = getattr(storage_adapter, "_backend", None)
        if backend is None or not hasattr(backend, "get_today_stats_by_model"):
            return JSONResponse(content={
                "by_family": {},
                "totals": {"success": 0, "failure": 0, "total": 0, "rpm": 0},
                "note": "当前存储后端不支持按模型统计",
            })
        data = await backend.get_today_stats_by_model(mode=mode)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取今日按模型统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
