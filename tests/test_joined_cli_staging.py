from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from atlas_io import joined_cli


def test_failed_join_preserves_incomplete_staging_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "assembly"

    monkeypatch.setattr(
        joined_cli,
        "resolve_hub_file_size",
        lambda *_args, **_kwargs: 1,
    )

    def fail_after_partial_stage(
        _source: Any,
        table: str,
        _path: str,
        *,
        size_bytes: int,
        max_bytes: int,
        opener: Any,
        directory: Path,
        progress_callback: Any = None,
    ) -> Any:
        partial = directory / f"{table}.parquet"
        partial.write_bytes(b"partial parquet stage")
        if progress_callback is not None:
            progress_callback(f"table={table} stage=failed")
        raise TimeoutError("simulated remote read timeout")

    monkeypatch.setattr(joined_cli, "_stage_rows", fail_after_partial_stage)

    exit_code = joined_cli.main(
        [
            "--source",
            "website",
            "--labels",
            "labels/polygons/temporary-fixture.parquet",
            "--max-bytes",
            "1000",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == joined_cli.EXIT_INPUT_ERROR
    staging_dirs = list(output_dir.glob(".atlas-join-staging-*"))
    assert len(staging_dirs) == 1
    assert (staging_dirs[0] / "labels.parquet").read_bytes() == b"partial parquet stage"
    assert "preserved incomplete staging directory" in capsys.readouterr().err
