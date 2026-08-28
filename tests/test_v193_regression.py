import json

from src.api.utils import is_license_error
from src.converter.openai2gemini import convert_gemini_to_openai_stream


FULL_LICENSE_ERROR = (
    "You do not have a valid license of this product. Please contact your administrator "
    "to request a license. If you are not an enterprise user and believe you are receiving "
    "this message as an error, please try using the latest version and logging in again."
)


def _decode_sse(value: str) -> dict:
    assert value.startswith("data: ")
    return json.loads(value[len("data: "):].strip())


def test_full_license_error_is_classified():
    assert is_license_error(FULL_LICENSE_ERROR)
    assert not is_license_error("Resource has been exhausted")


def test_stream_intermediate_chunk_has_null_finish_reason():
    gemini_chunk = {
        "candidates": [{
            "index": 0,
            "content": {"role": "model", "parts": [{"text": "thinking", "thought": True}]},
        }]
    }
    result = _decode_sse(convert_gemini_to_openai_stream(
        f"data: {json.dumps(gemini_chunk)}", "gemini-3.5-flash", "resp-1"
    ))
    assert result["choices"][0]["finish_reason"] is None
    assert result["choices"][0]["delta"]["reasoning_content"] == "thinking"


def test_stream_final_tool_chunk_finishes_as_tool_calls():
    gemini_chunk = {
        "candidates": [{
            "index": 0,
            "finishReason": "STOP",
            "content": {"role": "model", "parts": [{
                "functionCall": {"name": "lookup", "args": {"q": "x"}}
            }]},
        }]
    }
    result = _decode_sse(convert_gemini_to_openai_stream(
        f"data: {json.dumps(gemini_chunk)}", "gemini-3.5-flash", "resp-2"
    ))
    assert result["choices"][0]["finish_reason"] == "tool_calls"
    assert result["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "lookup"
