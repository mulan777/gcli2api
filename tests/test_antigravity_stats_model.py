import inspect

from src.api import antigravity
from src.api.antigravity import antigravity_stats_model_name
from src.storage.psql_manager import normalize_model_family


def test_gemini_37_stats_use_real_model_family_not_shared_cooldown_key():
    stats_model = antigravity_stats_model_name("gemini-3.7-flash")

    assert stats_model == "gemini-3.7-flash-medium"
    assert normalize_model_family(stats_model) == "3.7-flash"


def test_claude_opus_stats_use_real_model_family_not_shared_cooldown_key():
    stats_model = antigravity_stats_model_name("claude-opus-4-6-thinking")

    assert stats_model == "claude-opus-4-6-thinking"
    assert normalize_model_family(stats_model) == "claude-opus-4-6"


def test_antigravity_keeps_selection_and_statistics_keys_separate():
    source = inspect.getsource(antigravity)

    assert "get_valid_credential(\n        mode=\"antigravity\", model_name=cooldown_key" in source
    assert "record_api_call_success(\n                            credential_manager, current_file, mode=\"antigravity\", model_name=stats_model_name" in source
    assert "record_api_call_success(\n                        credential_manager, current_file, mode=\"antigravity\", model_name=stats_model_name" in source
    assert "record_api_call_success(\n                            credential_manager, current_file, mode=\"antigravity\", model_name=cooldown_key" not in source