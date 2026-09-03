"""FF-01: dependencies point inward; adapters are independent; provider libraries stay at the edge."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import REPO_ROOT, SRC_ROOT

pytestmark = pytest.mark.architecture

PROVIDERS = {
    "sqlglot",
    "duckdb",
    "pyarrow",
    "pyspark",
    "pandas",
    "polars",
    "fsspec",
    "adlfs",
    "s3fs",
    "paramiko",
    "azure",
    "boto3",
    "botocore",
    "psycopg",
    "pymysql",
    "pymongo",
    "bson",
    "neo4j",
    "deltalake",
    "pyiceberg",
    "numpy",
}
STDLIB = set(sys.stdlib_module_names)


def _modules() -> dict[str, Path]:
    return {
        ".".join(p.relative_to(SRC_ROOT.parent).with_suffix("").parts): p
        for p in SRC_ROOT.rglob("*.py")
    }


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.add(node.module)
    return found


def _layer(module: str) -> str:
    parts = module.split(".")
    return parts[1] if len(parts) > 1 else "root"


def _adapter_package(module: str) -> str | None:
    parts = module.split(".")
    if len(parts) >= 3 and parts[1] == "adapters":
        return parts[2]
    return None


ALLOWED_INTERNAL = {
    "domain": {"domain"},
    "ports": {"domain", "ports"},
    "application": {"domain", "ports", "application"},
    "api": {"domain", "ports", "application", "api"},
    "adapters": {"domain", "ports", "adapters"},
    "root": {"api", "domain"},
}


@pytest.mark.parametrize("module", sorted(_modules()))
def test_module_obeys_layer_rules(module: str) -> None:
    path = _modules()[module]
    layer = _layer(module) if module != "invariantql.__init__" else "root"
    if module == "invariantql":
        layer = "root"
    for imported in _imports(path):
        top = imported.split(".")[0]
        if top == "invariantql":
            target_layer = _layer(imported)
            assert target_layer in ALLOWED_INTERNAL[layer], (
                f"{module} -> {imported} violates layer rules"
            )
            if layer == "adapters":
                mine, theirs = _adapter_package(module), _adapter_package(imported)
                if theirs is not None and theirs != "_shared":
                    assert mine == theirs, f"adapter {module} imports sibling adapter {imported}"
        elif top in STDLIB or top == "__future__":
            continue
        else:
            assert layer == "adapters", f"{module} ({layer}) imports third-party {imported}"
            assert top in PROVIDERS or top in {"typing_extensions"}, (
                f"{module} imports unexpected third-party {imported}"
            )


def test_import_linter_contracts_pass() -> None:
    executable = Path(sys.executable).with_name("lint-imports")
    if not executable.exists():
        pytest.skip("import-linter is not installed in this environment")
    result = subprocess.run(
        [str(executable)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "KEPT" in result.stdout
    assert "BROKEN" not in result.stdout
