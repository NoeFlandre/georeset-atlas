from __future__ import annotations

import argparse
import ast
from importlib.util import resolve_name
from pathlib import Path

_FORBIDDEN_DEPENDENCIES = {
    "atlas_domain": frozenset({"atlas_io", "atlas_h3"}),
    "atlas_io": frozenset({"atlas_h3"}),
}


def check_architecture(root: Path) -> tuple[str, ...]:
    source_root = root / "src"
    module_paths = _module_paths(source_root)
    if not module_paths:
        return ("no Python source modules found under src",)
    dependencies = _dependencies(module_paths)
    findings = _layer_findings(dependencies)
    findings.extend(_cycle_findings(dependencies))
    return tuple(sorted(findings))


def _module_paths(source_root: Path) -> dict[str, Path]:
    modules: dict[str, Path] = {}
    for path in sorted(source_root.rglob("*.py")):
        relative = path.relative_to(source_root)
        parts = (
            relative.parts[:-1]
            if path.name == "__init__.py"
            else relative.with_suffix("").parts
        )
        if parts:
            modules[".".join(parts)] = path
    return modules


def _dependencies(module_paths: dict[str, Path]) -> dict[str, set[str]]:
    known_modules = set(module_paths)
    graph = {module: set() for module in module_paths}
    for module, path in module_paths.items():
        package = module if path.name == "__init__.py" else module.rpartition(".")[0]
        for node in ast.walk(
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        ):
            if isinstance(node, ast.Import):
                candidates = {alias.name for alias in node.names}
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    prefix = f"{'.' * node.level}{node.module or ''}"
                    base = resolve_name(prefix, package) if package else node.module
                else:
                    base = node.module
                if base is None:
                    continue
                candidates = {base}
                candidates.update(
                    f"{base}.{alias.name}" for alias in node.names if alias.name != "*"
                )
            else:
                continue
            graph[module].update(
                target
                for candidate in candidates
                if (target := _known_module(candidate, known_modules)) is not None
                and target != module
            )
    return graph


def _known_module(candidate: str, known_modules: set[str]) -> str | None:
    parts = candidate.split(".")
    for end in range(len(parts), 0, -1):
        module = ".".join(parts[:end])
        if module in known_modules:
            return module
    return None


def _layer_findings(dependencies: dict[str, set[str]]) -> list[str]:
    findings = []
    for importer, imports in dependencies.items():
        package = importer.split(".", 1)[0]
        forbidden = _FORBIDDEN_DEPENDENCIES.get(package, frozenset())
        findings.extend(
            f"{importer} depends on forbidden layer {dependency}"
            for dependency in imports
            if dependency.split(".", 1)[0] in forbidden
        )
    return findings


def _cycle_findings(dependencies: dict[str, set[str]]) -> list[str]:
    completed: set[str] = set()
    active: list[str] = []
    active_positions: dict[str, int] = {}
    cycles: set[tuple[str, ...]] = set()

    def visit(module: str) -> None:
        active_positions[module] = len(active)
        active.append(module)
        for dependency in sorted(dependencies[module]):
            if dependency in active_positions:
                cycle = active[active_positions[dependency] :]
                rotations = [
                    tuple(cycle[index:] + cycle[:index]) for index in range(len(cycle))
                ]
                cycles.add(min(rotations))
            elif dependency not in completed:
                visit(dependency)
        active.pop()
        active_positions.pop(module)
        completed.add(module)

    for module in sorted(dependencies):
        if module not in completed:
            visit(module)
    return [
        "internal import cycle: " + " -> ".join((*cycle, cycle[0]))
        for cycle in sorted(cycles)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check-architecture",
        description="Check Atlas package-layer rules and internal import cycles.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    args = parser.parse_args(argv)
    findings = check_architecture(args.root)
    if findings:
        for finding in findings:
            print(finding)
        return 1
    print(f"architecture check passed: {_module_count(args.root)} modules")
    return 0


def _module_count(root: Path) -> int:
    return len(_module_paths(root / "src"))


if __name__ == "__main__":
    raise SystemExit(main())
