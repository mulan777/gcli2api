import pytest

from src.converter.gemini_fix import normalize_gemini_request
from src.panel.creds import _build_unique_refresh_filename


@pytest.mark.asyncio
async def test_gemini_35_flash_adds_include_thoughts(monkeypatch):
    async def _yes():
        return True

    monkeypatch.setattr(
        "config.get_return_thoughts_to_frontend",
        _yes,
    )

    result = await normalize_gemini_request(
        {
            "model": "gemini-3.5-flash",
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "generationConfig": {},
        }
    )

    thinking = result["generationConfig"]["thinkingConfig"]
    assert thinking["includeThoughts"] is True
    assert "thinkingLevel" not in thinking
    assert "thinkingBudget" not in thinking


@pytest.mark.asyncio
async def test_gemini_31_pro_keeps_return_thoughts(monkeypatch):
    async def _yes():
        return True

    monkeypatch.setattr(
        "config.get_return_thoughts_to_frontend",
        _yes,
    )

    result = await normalize_gemini_request(
        {
            "model": "gemini-3.1-pro-preview",
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "generationConfig": {},
        }
    )
    assert result["generationConfig"]["thinkingConfig"]["includeThoughts"] is True


@pytest.mark.asyncio
async def test_gemini_25_flash_does_not_force_thoughts(monkeypatch):
    async def _yes():
        return True

    monkeypatch.setattr(
        "config.get_return_thoughts_to_frontend",
        _yes,
    )

    result = await normalize_gemini_request(
        {
            "model": "gemini-2.5-flash",
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "generationConfig": {},
        }
    )
    thinking = (result.get("generationConfig") or {}).get("thinkingConfig") or {}
    assert thinking.get("includeThoughts") is not True


def test_refresh_filename_unique_and_not_project_id():
    a = _build_unique_refresh_filename("1//token-aaa", None)
    b = _build_unique_refresh_filename("1//token-bbb", None)
    assert a != b
    assert a.startswith("refresh-") and a.endswith(".json")
    assert b.startswith("refresh-") and b.endswith(".json")
    assert "a01024779011" not in a
    prefixed = _build_unique_refresh_filename("1//token-ccc", "batch-01")
    assert prefixed == "batch-01.json"
