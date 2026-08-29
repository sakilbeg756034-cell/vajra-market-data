from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from vajra_regime import paths


class EnvironmentSettings(BaseSettings):
    """Machine-specific paths loaded from environment variables or .env."""

    model_config = SettingsConfigDict(
        env_prefix="VAJRA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    root: Path = Field(default_factory=lambda: paths.STORE_ROOT)
    master_data_root: Path = Field(default_factory=lambda: paths.MASTER_DATA)
    duckdb_path: Path = Field(default_factory=lambda: paths.MASTER_DB)
    logs_dir: Path = Field(default_factory=lambda: paths.STORE_LOGS)
    backup_dir: Path = Field(default_factory=lambda: paths.BACKUPS)
    published_data_root: Path = Field(default_factory=lambda: paths.DATA_ROOT)
    config_path: Path = Field(default=Path("config/default.yaml"))


@dataclass(frozen=True)
class AppConfig:
    environment: EnvironmentSettings
    raw: dict[str, Any]

    @property
    def data(self) -> dict[str, Any]:
        return self.raw["data"]

    @property
    def features(self) -> dict[str, Any]:
        return self.raw["features"]

    @property
    def regime(self) -> dict[str, Any]:
        return self.raw["regime"]

    @property
    def strategy(self) -> dict[str, Any]:
        return self.raw["strategy"]

    @property
    def research(self) -> dict[str, Any]:
        return self.raw["research"]


def load_config(path: Path | None = None) -> AppConfig:
    env = EnvironmentSettings()
    config_path = path or env.config_path
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            "Run from the repository root or set VAJRA_CONFIG_PATH."
        )
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    # The strategy, regime and research sections went with the code that read them.
    required = {"project", "data"}
    missing = required.difference(raw or {})
    if missing:
        raise ValueError(f"Config is missing sections: {sorted(missing)}")
    return AppConfig(environment=env, raw=raw)
