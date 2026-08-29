from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataLayout:
    """On-disk layout of the engine's working store.

    The numbered folder names are inherited and deliberately unchanged: every checkpoint,
    manifest and status file written since 2026-08 records these exact strings, and renaming
    them would break that provenance trail for no gain.

    The store holds working data only. The clean, published dataset lives somewhere else
    entirely; see the publish root in vajra_regime.paths.
    """

    root: Path
    protected_source: Path
    master_data: Path
    incoming_eod: Path
    corporate_actions: Path
    logs: Path
    backups: Path

    @classmethod
    def from_root(cls, root: Path) -> "DataLayout":
        root = Path(root)
        return cls(
            root=root,
            protected_source=root / "01 Protected Source Data",
            master_data=root / "02 Master Historical Data",
            incoming_eod=root / "03 Incoming NSE EOD",
            corporate_actions=root / "04 Corporate Actions",
            logs=root / "08 Logs",
            backups=root / "09 Backups",
        )

    def directories(self) -> tuple[Path, ...]:
        return (
            self.root,
            self.protected_source,
            self.master_data,
            self.incoming_eod,
            self.corporate_actions,
            self.logs,
            self.backups,
        )

    def create(self) -> None:
        for directory in self.directories():
            directory.mkdir(parents=True, exist_ok=True)

    def as_dict(self) -> dict[str, str]:
        return {
            "root": str(self.root),
            "protected_source": str(self.protected_source),
            "master_data": str(self.master_data),
            "incoming_eod": str(self.incoming_eod),
            "corporate_actions": str(self.corporate_actions),
            "logs": str(self.logs),
            "backups": str(self.backups),
        }
