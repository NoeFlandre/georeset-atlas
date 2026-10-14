import re
from dataclasses import dataclass

_COMMIT_SHA = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True, slots=True)
class PinnedHubParquetSource:
    repo_id: str
    revision: str
    path: str
    size_bytes: int

    def __post_init__(self) -> None:
        repo_parts = self.repo_id.split("/")
        if len(repo_parts) != 2 or any(part in {"", ".", ".."} for part in repo_parts):
            raise ValueError("repo_id must contain an owner and repository name")
        if _COMMIT_SHA.fullmatch(self.revision) is None:
            raise ValueError("revision must be a 40-character lowercase commit SHA")
        if (
            not self.path
            or self.path.startswith("/")
            or "\\" in self.path
            or any(part in {"", ".", ".."} for part in self.path.split("/"))
        ):
            raise ValueError("path must be a relative repository file path")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes <= 0
        ):
            raise ValueError("size_bytes must be a positive integer")
