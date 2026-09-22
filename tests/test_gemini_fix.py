import pytest

from src.converter.antigravity_fix import (
    _ensure_empty_tool_schema_for_claude,
    _normalize_claude_text_parts,
    normalize_antigravity_request,
)
from src.utils import ANTIGRAVITY_USER_AGENT, normalize_antigravity_model_alias


def test_antigravity_claude_tools_keep_schema_in_parameters():
    tools = [
        {
            "functionDeclarations": [
                {
                    "name": "test_tool",
                    "description": "A test tool.",
                    "parametersJsonSchema": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                    },
                }
            ]
        }
    ]

    result = _ensure_empty_tool_schema_for_claude(tools, "claude-opus-4-6-thinking", "antigravity")
    declaration = result[0]["functionDeclarations"][0]

    assert declaration["parameters"]["type"] == "object"
    assert "parametersJsonSchema" not in declaration


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model_id,stripped",
    [
        ("gemini-3.1-pro-high", "gemini-3.1-pro"),
        ("gemini-3.5-flash-high", "gemini-3.5-flash"),
        ("gemini-3.7-flash-low", "gemini-3.7-flash"),
        ("gemini-3.7-flash-medium", "gemini-3.7-flash"),
        ("gemini-3.7-flash-high", "gemini-3.7-flash"),
    ],
)
async def test_normalize_antigravity_keeps_native_model_ids(model_id, stripped, monkeypatch):
    monkeypatch.setenv("RETURN_THOUGHTS_TO_FRONTEND", "true")

    result = await normalize_antigravity_request(
        {
            "model": model_id,
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "generationConfig": {},
        }
    )

    assert result["model"] == model_id
    assert result["model"] != stripped


def test_antigravity_uses_version_gated_cli_fingerprint():
    assert ANTIGRAVITY_USER_AGENT == (
        "antigravity/cli/1.1.24 "
        "(aidev_client; os_type=windows; arch=amd64)"
    )


def test_antigravity_bare_gemini_37_defaults_to_medium_wire_id():
    assert normalize_antigravity_model_alias("gemini-3.7-flash") == (
        "gemini-3.7-flash-medium"
    )
    for effort in ("low", "medium", "high"):
        model_id = f"gemini-3.7-flash-{effort}"
        assert normalize_antigravity_model_alias(model_id) == model_id


@pytest.mark.asyncio
async def test_gemini_37_drops_trailing_model_turn(monkeypatch):
    monkeypatch.setenv("RETURN_THOUGHTS_TO_FRONTEND", "true")

    result = await normalize_antigravity_request(
        {
            "model": "gemini-3.7-flash-medium",
            "contents": [
                {"role": "user", "parts": [{"text": "first question"}]},
                {"role": "model", "parts": [{"text": "previous answer"}]},
            ],
            "generationConfig": {},
        }
    )

    assert result["contents"] == [
        {"role": "user", "parts": [{"text": "first question"}]}
    ]



def test_claude_nested_text_is_flattened_without_touching_other_parts():
    contents = [
        {
            "role": "user",
            "parts": [
                {"text": {"text": "hello"}},
                {"inlineData": {"mimeType": "image/png", "data": "abc"}},
            ],
        }
    ]

    assert _normalize_claude_text_parts(contents) == [
        {
            "role": "user",
            "parts": [
                {"text": "hello"},
                {"inlineData": {"mimeType": "image/png", "data": "abc"}},
            ],
        }
    ]


def test_claude_empty_text_part_is_removed():
    contents = [
        {
            "role": "user",
            "parts": [{"text": {}}, {"text": "  "}, {"text": "valid"}],
        }
    ]

    assert _normalize_claude_text_parts(contents) == [
        {"role": "user", "parts": [{"text": "valid"}]}
    ]


def test_claude_text_list_is_flattened():
    contents = [{"role": "user", "parts": [{"text": ["one", {"text": "two"}]}]}]

    assert _normalize_claude_text_parts(contents) == [
        {"role": "user", "parts": [{"text": "one two"}]}
    ]


@pytest.mark.asyncio
async def test_gemini_38_drops_trailing_model_turn(monkeypatch):
    monkeypatch.setenv("RETURN_THOUGHTS_TO_FRONTEND", "true")

    result = await normalize_antigravity_request(
        {
            "model": "gemini-3.8-flash-medium",
            "contents": [
                {"role": "user", "parts": [{"text": "first question"}]},
                {"role": "model", "parts": [{"text": "previous answer"}]},
            ],
            "generationConfig": {},
        }
    )

    assert result["contents"] == [
        {"role": "user", "parts": [{"text": "first question"}]}
    ]
