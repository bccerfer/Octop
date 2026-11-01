"""Optional deployment data only: no artifact discovery, execution or persistence."""

import json
import os
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from unittest.mock import patch

import pytest

from octop.config import MemoryLakeCliConfig, OctopConfig, load_config
from tests.support.memorylake import offline_test_guard


@pytest.fixture(autouse=True)
def isolated():
    with offline_test_guard(), patch.dict(os.environ, {}, clear=True):
        yield


def config_file(tmp_path, section):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"port": 9012, "memorylake_cli": section}))
    return path


def test_defaults_frozen_and_no_optional_persistence(tmp_path):
    assert OctopConfig().memorylake_cli == MemoryLakeCliConfig()
    with pytest.raises(FrozenInstanceError):
        OctopConfig().memorylake_cli.enabled = True
    path = tmp_path / "config.json"
    assert load_config(path).memorylake_cli == MemoryLakeCliConfig()
    assert "memorylake_cli" not in json.loads(path.read_text())
    assert asdict(OctopConfig())["memorylake_cli"]["enabled"] is False


def test_file_and_env_precedence_without_save_or_artifact_probe(tmp_path, monkeypatch):
    path = config_file(
        tmp_path,
        {
            "enabled": True,
            "executable": "missing-file",
            "expected_version": "v1",
            "sha256": "a" * 64,
        },
    )
    original = path.read_bytes()
    exists = Path.exists

    def checked_exists(candidate):
        assert candidate == path, "artifact must not be probed"
        return exists(candidate)

    with (
        patch.object(Path, "exists", checked_exists),
        patch("shutil.which", side_effect=AssertionError("PATH search")),
        patch("subprocess.Popen", side_effect=AssertionError("execute")),
    ):
        cfg = load_config(path)
        assert cfg.memorylake_cli == MemoryLakeCliConfig(True, "missing-file", "v1", "a" * 64)
        monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_EXECUTABLE", "other-missing-file")
        monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_EXPECTED_VERSION", "v2")
        monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_SHA256", "b" * 64)
        monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_ENABLED", "false")
        cfg = load_config(path)
        assert cfg.memorylake_cli == MemoryLakeCliConfig(
            False, "other-missing-file", "v2", "b" * 64
        )
    assert cfg.port == 9012
    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        "sentinel",
        1,
        {"unknown": "sentinel"},
        {"enabled": "false"},
        {"enabled": 1},
        {"executable": 17},
        {"sha256": {}},
        {"expected_version": []},
        {"executable": "s" * 4097},
        {"expected_version": "s" * 129},
        {"sha256": "s" * 65},
    ],
)
def test_malformed_subsection_disables_only_feature_without_echo(
    tmp_path, raw, caplog, monkeypatch
):
    path = config_file(tmp_path, raw)
    original = path.read_bytes()
    monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_ENABLED", "true")
    cfg = load_config(path)
    assert cfg.memorylake_cli == MemoryLakeCliConfig()
    assert cfg.port == 9012
    assert path.read_bytes() == original
    assert [r.getMessage() for r in caplog.records] == [
        "Invalid MemoryLake CLI configuration; feature disabled"
    ]


@pytest.mark.parametrize(
    "value,enabled", [("true", True), ("false", False), ("1", True), ("0", False)]
)
def test_explicit_env_boolean(tmp_path, monkeypatch, value, enabled):
    path = config_file(tmp_path, {})
    monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_ENABLED", value)
    assert load_config(path).memorylake_cli.enabled is enabled


@pytest.mark.parametrize("value", ["", "yes", "sentinel", "2"])
def test_invalid_env_boolean_failclosed(tmp_path, monkeypatch, caplog, value):
    path = config_file(tmp_path, {"enabled": True})
    monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_ENABLED", value)
    assert load_config(path).memorylake_cli == MemoryLakeCliConfig()
    assert [r.getMessage() for r in caplog.records] == [
        "Invalid MemoryLake CLI configuration; feature disabled"
    ]


def test_partial_enabled_and_boundaries_are_data_not_verified_artifact(tmp_path):
    assert load_config(config_file(tmp_path, {"enabled": True})).memorylake_cli.enabled
    section = {
        "enabled": True,
        "executable": "p" * 4096,
        "expected_version": "v" * 128,
        "sha256": "h" * 64,
    }
    assert asdict(load_config(config_file(tmp_path, section)).memorylake_cli) == section


def test_oversized_environment_disables(tmp_path, monkeypatch):
    path = config_file(tmp_path, {"enabled": True})
    monkeypatch.setenv("OCTOP_MEMORYLAKE_CLI_SHA256", "s" * 65)
    assert load_config(path).memorylake_cli == MemoryLakeCliConfig()
