import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.check_architecture import check_architecture


def test_architecture_check_accepts_domain_depended_on_by_io(tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "atlas_domain").mkdir(parents=True)
    (source / "atlas_io").mkdir()
    (source / "atlas_domain" / "__init__.py").write_text("", encoding="utf-8")
    (source / "atlas_domain" / "record.py").write_text("value = 1\n", encoding="utf-8")
    (source / "atlas_io" / "__init__.py").write_text("", encoding="utf-8")
    (source / "atlas_io" / "reader.py").write_text(
        "from atlas_domain.record import value\n",
        encoding="utf-8",
    )

    assert check_architecture(tmp_path) == ()


def test_architecture_check_reports_forbidden_layer_import(tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "atlas_domain").mkdir(parents=True)
    (source / "atlas_io").mkdir()
    (source / "atlas_domain" / "__init__.py").write_text("", encoding="utf-8")
    (source / "atlas_domain" / "record.py").write_text(
        "from atlas_io.reader import read\n",
        encoding="utf-8",
    )
    (source / "atlas_io" / "__init__.py").write_text("", encoding="utf-8")
    (source / "atlas_io" / "reader.py").write_text(
        "def read():\n    return 1\n", encoding="utf-8"
    )

    assert check_architecture(tmp_path) == (
        "atlas_domain.record depends on forbidden layer atlas_io.reader",
    )


def test_architecture_check_reports_internal_import_cycle(tmp_path: Path) -> None:
    source = tmp_path / "src"
    (source / "atlas_domain").mkdir(parents=True)
    (source / "atlas_io").mkdir()
    (source / "atlas_domain" / "__init__.py").write_text("", encoding="utf-8")
    (source / "atlas_domain" / "record.py").write_text(
        "from atlas_io.reader import read\n",
        encoding="utf-8",
    )
    (source / "atlas_io" / "__init__.py").write_text("", encoding="utf-8")
    (source / "atlas_io" / "reader.py").write_text(
        "from atlas_domain.record import value\n",
        encoding="utf-8",
    )

    assert check_architecture(tmp_path) == (
        "atlas_domain.record depends on forbidden layer atlas_io.reader",
        "internal import cycle: atlas_domain.record -> atlas_io.reader -> "
        "atlas_domain.record",
    )


def test_canonical_repository_has_no_architecture_violations() -> None:
    repository = Path(__file__).resolve().parents[1]

    assert check_architecture(repository) == ()
