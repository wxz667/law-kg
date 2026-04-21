from __future__ import annotations

from dataclasses import dataclass


@dataclass
class NormalizeRunRecord:
    source_id: str
    status: str
    title: str = ""
    document: str = ""
    error_type: str = ""
    message: str = ""
    reused: bool = False
