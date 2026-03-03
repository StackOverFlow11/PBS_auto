"""Configuration loading and validation."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import tomli


DEFAULT_CONFIG_PATH = Path.home() / ".config" / "pbs_auto" / "config.toml"
DEFAULT_STATE_DIR = Path.home() / ".local" / "share" / "pbs_auto" / "batches"

DEFAULT_CONFIG_CONTENT = """\
[defaults]
server = "server1"
script_name = "script.sh"
poll_interval = 15
submit_delay = 2
post_submit_check_delay = 60
early_exit_threshold = 30

[servers.server1]
name = "Chemistry Department"
status_command = "qstat"
status_args = ["-au", "$USER"]
max_running_cores = 192
max_queued_cores = 192
core_granularity = 24

[servers.server1.queues.debug]
max_cores = 24
max_nodes = 1
max_walltime_hours = 0.5

[servers.server1.queues.short]
max_cores = 48
max_nodes = 1
max_walltime_hours = 168

[servers.server1.queues.medium]
max_cores = 96
min_cores = 24
allowed_cores = [24, 48, 72, 96]
max_nodes = 1
max_walltime_hours = 240

[servers.server1.queues.long]
max_cores = 192
min_cores = 48
allowed_cores = [48, 96, 144, 192]
max_nodes = -1
max_walltime_hours = 360

[servers.server2]
name = "Group Server"
status_command = "qstat"
status_args = ["-au", "$USER"]
max_running_cores = 240
max_queued_cores = 96
core_granularity = 24

[servers.server2.queues.medium]
max_cores = 96
min_cores = 24
allowed_cores = [24, 48, 96]
max_nodes = 1
max_walltime_hours = 360

[servers.server2.queues.long]
max_cores = 192
min_cores = 48
allowed_cores = [48, 96, 192]
max_nodes = -1
max_walltime_hours = 360
"""


@dataclass
class QueueConfig:
    """Configuration for a single PBS queue."""

    name: str
    max_cores: int
    min_cores: int = 0
    allowed_cores: list[int] | None = None
    max_nodes: int = 1
    max_walltime_hours: float = 360.0


@dataclass
class ServerConfig:
    """Configuration for a specific server/cluster."""

    name: str
    status_command: str = "qstat"
    status_args: list[str] = field(default_factory=lambda: ["-au", "$USER"])
    max_running_cores: int = 240
    max_queued_cores: int = 192
    core_granularity: int = 24
    queues: dict[str, QueueConfig] = field(default_factory=dict)


@dataclass
class AppConfig:
    """Application-wide configuration."""

    server: str = "server1"
    script_name: str = "script.sh"
    poll_interval: int = 15
    submit_delay: int = 2
    post_submit_check_delay: int = 60
    early_exit_threshold: int = 30
    servers: dict[str, ServerConfig] = field(default_factory=dict)

    def get_server(self, name: str | None = None) -> ServerConfig:
        key = name or self.server
        if key not in self.servers:
            available = ", ".join(self.servers.keys()) or "(none)"
            raise ValueError(
                f"Server profile '{key}' not found. Available: {available}"
            )
        return self.servers[key]


def find_config_path(cli_path: str | None = None) -> Path | None:
    """Find config file path using priority: CLI arg > env var > default."""
    if cli_path:
        p = Path(cli_path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        return p

    env_path = os.environ.get("PBS_AUTO_CONFIG")
    if env_path:
        p = Path(env_path)
        if not p.exists():
            raise FileNotFoundError(
                f"Config file from $PBS_AUTO_CONFIG not found: {p}"
            )
        return p

    if DEFAULT_CONFIG_PATH.exists():
        return DEFAULT_CONFIG_PATH

    return None


def load_config(cli_path: str | None = None) -> AppConfig:
    """Load configuration from TOML file, falling back to defaults."""
    config_path = find_config_path(cli_path)

    if config_path is None:
        return _build_default_config()

    with open(config_path, "rb") as f:
        raw = tomli.load(f)

    return _parse_config(raw)


def _build_default_config() -> AppConfig:
    """Build config with sensible defaults when no config file exists."""
    raw = tomli.loads(DEFAULT_CONFIG_CONTENT)
    return _parse_config(raw)


def _parse_config(raw: dict) -> AppConfig:
    """Parse raw TOML dict into AppConfig."""
    defaults = raw.get("defaults", {})
    servers_raw = raw.get("servers", {})

    servers = {}
    for key, srv_data in servers_raw.items():
        queues_raw = srv_data.get("queues", {})
        queues = {}
        for q_name, q_data in queues_raw.items():
            queues[q_name] = QueueConfig(
                name=q_name,
                max_cores=q_data.get("max_cores", 0),
                min_cores=q_data.get("min_cores", 0),
                allowed_cores=q_data.get("allowed_cores"),
                max_nodes=q_data.get("max_nodes", 1),
                max_walltime_hours=q_data.get("max_walltime_hours", 360.0),
            )

        servers[key] = ServerConfig(
            name=srv_data.get("name", key),
            status_command=srv_data.get("status_command", "qstat"),
            status_args=srv_data.get("status_args", ["-au", "$USER"]),
            max_running_cores=srv_data.get("max_running_cores", 240),
            max_queued_cores=srv_data.get("max_queued_cores", 192),
            core_granularity=srv_data.get("core_granularity", 24),
            queues=queues,
        )

    return AppConfig(
        server=defaults.get("server", "server1"),
        script_name=defaults.get("script_name", "script.sh"),
        poll_interval=defaults.get("poll_interval", 15),
        submit_delay=defaults.get("submit_delay", 2),
        post_submit_check_delay=defaults.get("post_submit_check_delay", 60),
        early_exit_threshold=defaults.get("early_exit_threshold", 30),
        servers=servers,
    )


def init_config() -> Path:
    """Create default config file. Returns the path created."""
    DEFAULT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DEFAULT_CONFIG_PATH.exists():
        raise FileExistsError(
            f"Config file already exists: {DEFAULT_CONFIG_PATH}"
        )
    DEFAULT_CONFIG_PATH.write_text(DEFAULT_CONFIG_CONTENT)
    return DEFAULT_CONFIG_PATH
