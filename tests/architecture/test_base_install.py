"""FF-02: importing the package loads no provider SDK and emits no warning."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

pytestmark = pytest.mark.architecture

PROVIDERS = [
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
    "psycopg",
    "pymysql",
    "pymongo",
    "neo4j",
    "deltalake",
    "pyiceberg",
    "defusedxml",
    "sqlglot",
    "numpy",
]

SCRIPT = """
import json, sys, warnings
warnings.simplefilter("error")
import invariantql
import invariantql.api, invariantql.application, invariantql.domain, invariantql.ports
ctx = invariantql.Context()
q = ctx.query("orders").limit(1)
print(json.dumps(sorted(m.split('.')[0] for m in sys.modules)))
"""


def test_base_import_is_isolated() -> None:
    result = subprocess.run(
        [sys.executable, "-W", "error", "-c", SCRIPT], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    loaded = set(json.loads(result.stdout.strip().splitlines()[-1]))
    leaked = sorted(p for p in PROVIDERS if p in loaded)
    assert not leaked, f"base import loaded provider modules: {leaked}"
    assert "invariantql" in loaded
    assert not any(m.startswith("invariantql.adapters") for m in loaded)
