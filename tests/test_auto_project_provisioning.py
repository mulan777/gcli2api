import pytest

from src.google_oauth_api import Credentials, enable_required_apis, ensure_geminicli_project, validate_geminicli_project


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


@pytest.mark.asyncio
async def test_uses_active_v3_project_without_creating(monkeypatch):
    calls = []

    async def fake_get(url, **kwargs):
        calls.append(("GET", url))
        assert url.endswith("/v3/projects:search")
        return FakeResponse(200, {"projects": [{"projectId": "existing-project", "state": "ACTIVE"}]})

    async def fake_post(url, **kwargs):
        calls.append(("POST", url))
        raise AssertionError("已有项目时不应创建项目")

    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)
    monkeypatch.setattr("src.google_oauth_api.get_resource_manager_api_url", lambda: _async_value("https://cloudresourcemanager.googleapis.com"))

    result = await ensure_geminicli_project(Credentials("token"))

    assert result == "existing-project"
    assert not any(method == "POST" for method, _ in calls)


@pytest.mark.asyncio
async def test_keeps_accessible_project_already_stored_in_credential(monkeypatch):
    calls = []

    async def fake_get(url, **kwargs):
        calls.append(url)
        assert url.endswith("/v3/projects/stored-project")
        return FakeResponse(200, {"projectId": "stored-project", "state": "ACTIVE"})

    async def fake_post(url, **kwargs):
        raise AssertionError("已有可访问项目时不应创建项目")

    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)
    monkeypatch.setattr("src.google_oauth_api.get_resource_manager_api_url", lambda: _async_value("https://cloudresourcemanager.googleapis.com"))

    result = await ensure_geminicli_project(Credentials("token", project_id="stored-project"))

    assert result == "stored-project"
    assert calls == ["https://cloudresourcemanager.googleapis.com/v3/projects/stored-project"]


@pytest.mark.asyncio
async def test_creates_project_when_v3_search_is_empty(monkeypatch):
    calls = []

    async def fake_get(url, **kwargs):
        calls.append(("GET", url))
        if url.endswith("/v3/projects:search"):
            return FakeResponse(200, {})
        if "/v3/operations/create_project." in url:
            return FakeResponse(200, {
                "done": True,
                "response": {"projectId": "created-project", "state": "ACTIVE"},
            })
        raise AssertionError(f"意外GET: {url}")

    async def fake_post(url, **kwargs):
        calls.append(("POST", url))
        assert url.endswith("/v3/projects")
        body = kwargs["json"]
        assert body["projectId"].startswith("gemini-cli-")
        return FakeResponse(200, {"name": "operations/create_project.global.test"})

    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)
    monkeypatch.setattr("src.google_oauth_api.get_resource_manager_api_url", lambda: _async_value("https://cloudresourcemanager.googleapis.com"))
    monkeypatch.setattr("src.google_oauth_api.asyncio.sleep", _no_sleep)

    result = await ensure_geminicli_project(Credentials("token"))

    assert result == "created-project"
    assert any(method == "POST" and url.endswith("/v3/projects") for method, url in calls)


@pytest.mark.asyncio
async def test_second_search_reuses_project_created_by_concurrent_import(monkeypatch):
    searches = 0

    async def fake_get(url, **kwargs):
        nonlocal searches
        if url.endswith("/v3/projects:search"):
            searches += 1
            if searches == 1:
                return FakeResponse(200, {})
            return FakeResponse(200, {"projects": [{"projectId": "created-by-peer", "state": "ACTIVE"}]})
        raise AssertionError(f"意外GET: {url}")

    async def fake_post(url, **kwargs):
        raise AssertionError("二次查询已有项目时不得重复创建")

    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)
    monkeypatch.setattr("src.google_oauth_api.get_resource_manager_api_url", lambda: _async_value("https://cloudresourcemanager.googleapis.com"))

    assert await ensure_geminicli_project(Credentials("token")) == "created-by-peer"
    assert searches == 2


@pytest.mark.asyncio
async def test_v3_search_error_does_not_create_project(monkeypatch):
    async def fake_get(url, **kwargs):
        return FakeResponse(429, {"error": {"status": "RESOURCE_EXHAUSTED"}})

    async def fake_post(url, **kwargs):
        raise AssertionError("查询失败不是没有项目，不得创建")

    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)
    monkeypatch.setattr("src.google_oauth_api.get_resource_manager_api_url", lambda: _async_value("https://cloudresourcemanager.googleapis.com"))

    with pytest.raises(RuntimeError, match="项目查询失败"):
        await ensure_geminicli_project(Credentials("token"))


@pytest.mark.asyncio
async def test_enable_required_apis_reports_failure(monkeypatch):
    async def fake_base_url():
        return "https://serviceusage.googleapis.com"

    async def fake_get(url, **kwargs):
        return FakeResponse(200, {"state": "DISABLED"})

    async def fake_post(url, **kwargs):
        return FakeResponse(403, {"error": {"status": "PERMISSION_DENIED"}})

    monkeypatch.setattr("src.google_oauth_api.get_service_usage_api_url", fake_base_url)
    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)

    assert await enable_required_apis(Credentials("token"), "project-a") is False


@pytest.mark.asyncio
async def test_enable_required_apis_non_already_enabled_400_is_failure(monkeypatch):
    async def fake_base_url():
        return "https://serviceusage.googleapis.com"

    async def fake_get(url, **kwargs):
        return FakeResponse(200, {"state": "DISABLED"})

    async def fake_post(url, **kwargs):
        return FakeResponse(400, {"error": {"message": "billing account required"}})

    monkeypatch.setattr("src.google_oauth_api.get_service_usage_api_url", fake_base_url)
    monkeypatch.setattr("src.google_oauth_api.get_async", fake_get)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)

    assert await enable_required_apis(Credentials("token"), "project-a") is False


@pytest.mark.asyncio
async def test_real_geminicli_validation_requires_http_200(monkeypatch):
    async def fake_endpoint():
        return "https://cloudcode-pa.googleapis.com"

    async def fake_post(url, **kwargs):
        assert url.endswith("/v1internal:generateContent")
        assert kwargs["json"]["project"] == "project-a"
        return FakeResponse(403, {"error": {"status": "PERMISSION_DENIED"}})

    monkeypatch.setattr("src.google_oauth_api.get_code_assist_endpoint", fake_endpoint)
    monkeypatch.setattr("src.google_oauth_api.post_async", fake_post)

    assert await validate_geminicli_project(Credentials("token"), "project-a") is False


async def _async_value(value):
    return value


async def _no_sleep(_seconds):
    return None
