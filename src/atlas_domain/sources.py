import re
from dataclasses import dataclass

_COMMIT_SHA = re.compile(r"[0-9a-f]{40}")


def validate_pinned_reference(repo_id: str, revision: str) -> None:
    if not isinstance(repo_id, str) or "\\" in repo_id:
        raise ValueError("repo_id must contain an owner and repository name")
    repo_parts = repo_id.split("/")
    if len(repo_parts) != 2 or any(part in {"", ".", ".."} for part in repo_parts):
        raise ValueError("repo_id must contain an owner and repository name")
    if not isinstance(revision, str) or _COMMIT_SHA.fullmatch(revision) is None:
        raise ValueError("revision must be a 40-character lowercase commit SHA")


def validate_pinned_file(repo_id: str, revision: str, path: str) -> None:
    validate_pinned_reference(repo_id, revision)
    if not isinstance(path, str):
        raise ValueError("path must be a relative repository file path")
    if (
        not path
        or path.startswith("/")
        or "\\" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise ValueError("path must be a relative repository file path")


@dataclass(frozen=True, slots=True)
class PinnedHubParquetSource:
    repo_id: str
    revision: str
    path: str
    size_bytes: int

    def __post_init__(self) -> None:
        validate_pinned_file(self.repo_id, self.revision, self.path)
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes <= 0
        ):
            raise ValueError("size_bytes must be a positive integer")
