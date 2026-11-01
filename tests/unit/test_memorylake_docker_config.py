"""Static standard Compose opt-in contract; no image build or container start."""

from pathlib import Path

from tests.unit.test_docker_compose_database_env import _compose_environment_entries

KEYS = (
    "OCTOP_MEMORYLAKE_CLI_ENABLED",
    "OCTOP_MEMORYLAKE_CLI_EXECUTABLE",
    "OCTOP_MEMORYLAKE_CLI_EXPECTED_VERSION",
    "OCTOP_MEMORYLAKE_CLI_SHA256",
)


def test_existing_compose_passes_optional_values_without_injecting_defaults():
    root = Path(__file__).resolve().parents[2]
    for name in ("docker-compose.yml", "docker-compose.cn.yml"):
        text = (root / "docker" / name).read_text()
        env = _compose_environment_entries(text).splitlines()
        for key in KEYS:
            assert f"      - {key}" in env
            assert not any(line.startswith(f"      - {key}=") for line in env)
        # No newly mandatory host file bind or separate MemoryLake service.
        assert "MEMORYLAKE_CLI_HOST_PATH" not in text
        assert "  memorylake:" not in text


def test_default_entrypoint_stays_native_without_install_login_or_version_probe():
    root = Path(__file__).resolve().parents[2]
    entrypoint = (root / "docker" / "docker-entrypoint.sh").read_text()
    assert 'exec octop run --host 0.0.0.0 --port "$PORT"' in entrypoint
    assert "memorylake" not in entrypoint.lower()
