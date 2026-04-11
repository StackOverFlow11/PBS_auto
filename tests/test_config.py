"""Tests for config module."""

from __future__ import annotations

from pathlib import Path

import pytest

from pbs_auto.config import (
    AppConfig,
    QueueConfig,
    ServerConfig,
    _parse_config,
    load_config,
    init_config,
    DEFAULT_CONFIG_CONTENT,
)

import tomli


class TestParseConfig:
    def test_parse_minimal(self):
        raw = tomli.loads(DEFAULT_CONFIG_CONTENT)
        config = _parse_config(raw)

        assert config.server == "server1"
        assert config.script_name == "script.sh"
        assert config.poll_interval == 15
        assert config.submit_delay == 2
        assert "server1" in config.servers
        assert "server2" in config.servers

    def test_server_config_values(self):
        raw = tomli.loads(DEFAULT_CONFIG_CONTENT)
        config = _parse_config(raw)
        s1 = config.get_server("server1")

        assert s1.name == "Chemistry Department"
        assert s1.max_running_cores == 192
        assert s1.max_queued_cores == 192
        assert s1.core_granularity == 24

    def test_get_server_not_found(self):
        config = AppConfig(servers={})
        with pytest.raises(ValueError, match="not found"):
            config.get_server("nonexistent")

    def test_parse_custom_values(self):
        raw = {
            "defaults": {
                "server": "custom",
                "poll_interval": 30,
                "early_exit_threshold": 60,
            },
            "servers": {
                "custom": {
                    "name": "Custom",
                    "max_running_cores": 480,
                    "max_queued_cores": 384,
                }
            },
        }
        config = _parse_config(raw)
        assert config.poll_interval == 30
        assert config.early_exit_threshold == 60
        s = config.get_server("custom")
        assert s.max_running_cores == 480

    def test_skip_if_exists_default_empty(self):
        config = _parse_config({"defaults": {}, "servers": {}})
        assert config.skip_if_exists == []

    def test_skip_if_exists_list(self):
        raw = {
            "defaults": {"skip_if_exists": ["cal.out", "time", "*.done"]},
            "servers": {},
        }
        config = _parse_config(raw)
        assert config.skip_if_exists == ["cal.out", "time", "*.done"]

    def test_skip_if_exists_string_wrapped_to_list(self):
        """Single string values are wrapped into a one-element list."""
        raw = {
            "defaults": {"skip_if_exists": "cal.out"},
            "servers": {},
        }
        config = _parse_config(raw)
        assert config.skip_if_exists == ["cal.out"]


class TestQueueConfigParsing:
    def test_default_config_has_queues(self):
        raw = tomli.loads(DEFAULT_CONFIG_CONTENT)
        config = _parse_config(raw)
        s1 = config.get_server("server1")
        assert len(s1.queues) == 4
        assert "debug" in s1.queues
        assert "short" in s1.queues
        assert "medium" in s1.queues
        assert "long" in s1.queues

    def test_server1_queue_values(self):
        raw = tomli.loads(DEFAULT_CONFIG_CONTENT)
        config = _parse_config(raw)
        s1 = config.get_server("server1")

        debug = s1.queues["debug"]
        assert debug.max_cores == 24
        assert debug.max_nodes == 1
        assert debug.max_walltime_hours == 0.5
        assert debug.allowed_cores is None

        medium = s1.queues["medium"]
        assert medium.allowed_cores == [24, 48, 72, 96]
        assert medium.min_cores == 24
        assert medium.max_nodes == 1

        long = s1.queues["long"]
        assert long.allowed_cores == [48, 96, 144, 192]
        assert long.max_nodes == -1

    def test_server2_queues(self):
        raw = tomli.loads(DEFAULT_CONFIG_CONTENT)
        config = _parse_config(raw)
        s2 = config.get_server("server2")
        assert len(s2.queues) == 2
        assert "medium" in s2.queues
        assert "long" in s2.queues

    def test_no_queues_defaults_to_empty(self):
        raw = {
            "servers": {
                "bare": {
                    "name": "Bare Server",
                    "max_running_cores": 100,
                }
            },
        }
        config = _parse_config(raw)
        s = config.get_server("bare")
        assert s.queues == {}


class TestLoadConfig:
    def test_load_with_explicit_path(self, tmp_path):
        config_file = tmp_path / "test.toml"
        config_file.write_text(DEFAULT_CONFIG_CONTENT)
        config = load_config(str(config_file))
        assert config.server == "server1"

    def test_load_missing_explicit_path(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path.toml")

    def test_load_no_config_uses_defaults(self, monkeypatch):
        monkeypatch.delenv("PBS_AUTO_CONFIG", raising=False)
        # If default path doesn't exist, should still return defaults
        config = load_config(None)
        assert isinstance(config, AppConfig)
        assert config.server == "server1"


class TestInitConfig:
    def test_init_creates_file(self, tmp_path, monkeypatch):
        config_path = tmp_path / "config.toml"
        monkeypatch.setattr(
            "pbs_auto.config.DEFAULT_CONFIG_PATH", config_path
        )
        path = init_config()
        assert path.exists()
        content = path.read_text()
        assert "[defaults]" in content
        assert "[servers.server1]" in content

    def test_init_refuses_overwrite(self, tmp_path, monkeypatch):
        config_path = tmp_path / "config.toml"
        config_path.write_text("existing")
        monkeypatch.setattr(
            "pbs_auto.config.DEFAULT_CONFIG_PATH", config_path
        )
        with pytest.raises(FileExistsError):
            init_config()
