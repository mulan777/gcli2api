import pytest

from src.converter.antigravity_fix import (
    _ensure_empty_tool_schema_for_claude,
    normalize_antigravity_request,
)


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
