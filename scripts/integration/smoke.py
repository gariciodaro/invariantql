#!/usr/bin/env python3
"""Run small, read-only InvariantQL probes against locally configured services.

Set up from the repository root::

    uv sync --extra duckdb --extra postgres --extra mongodb --extra azure
    cp scripts/integration/.env.example scripts/integration/.env
    # Fill in only the services you want to exercise.
    uv run python scripts/integration/smoke.py postgres mongodb adls-json

With no target arguments, all three probes run. Each probe discovers the
source schema, prints InvariantQL's execution plan, and materializes only the
configured number of rows. Failures do not stop the remaining probes.

This file lives outside ``src/`` and the project's explicit sdist allow-list,
so it is not included in either the wheel or source distribution.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import invariantql as iql
from invariantql.domain.redaction import redact as redact_invariantql

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = SCRIPT_DIR / ".env"
TARGETS = ("postgres", "mongodb", "adls-json")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})
_SECRET_ENV_NAMES = frozenset(
    {
        "INVARIANTQL_POSTGRES_DSN",
        "INVARIANTQL_MONGODB_URI",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_STORAGE_SAS_TOKEN",
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_STORAGE_CLIENT_SECRET",
    }
)


class ConfigurationError(RuntimeError):
    """A missing or inconsistent local smoke-test setting."""


def load_dotenv(path: Path) -> bool:
    """Load a small, deliberately non-interpolating subset of dotenv syntax.

    Existing process environment variables win. Values may be unquoted or
    Python-style single/double quoted; ``#`` starts a comment only when it is
    the first non-whitespace character on a line. This preserves ``#``, ``=``,
    ``&`` and ``;`` inside passwords, URIs, SAS tokens, and connection strings.
    """

    if not path.exists():
        return False
    if not path.is_file():
        raise ConfigurationError(f"dotenv path is not a file: {path}")

    if os.name != "nt" and path.stat().st_mode & 0o077:
        print(
            f"WARNING: {path} is readable by group/other users; consider chmod 600 {path}",
            file=sys.stderr,
        )

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        key = key.strip()
        if not separator or not _ENV_NAME.fullmatch(key):
            raise ConfigurationError(f"invalid dotenv assignment at {path}:{line_number}")
        value = _parse_dotenv_value(raw_value.strip(), path=path, line_number=line_number)
        os.environ.setdefault(key, value)
    return True


def _parse_dotenv_value(raw: str, *, path: Path, line_number: int) -> str:
    if not raw:
        return ""
    if raw[0] not in {'"', "'"}:
        return raw
    try:
        value = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        raise ConfigurationError(f"invalid quoted value at {path}:{line_number}") from None
    if not isinstance(value, str):
        raise ConfigurationError(f"dotenv values must be strings at {path}:{line_number}")
    return value


def _required(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ConfigurationError(f"set {name} in {DEFAULT_ENV_FILE} (or the process environment)")
    return value.strip()


def _optional(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _integer(name: str, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    raw = _optional(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from None
    if value < minimum or (maximum is not None and value > maximum):
        interval = f"{minimum}..{maximum}" if maximum is not None else f">= {minimum}"
        raise ConfigurationError(f"{name} must be {interval}, got {value}")
    return value


def _boolean(name: str, default: bool = False) -> bool:
    raw = _optional(name)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise ConfigurationError(f"{name} must be true/false, 1/0, yes/no, or on/off")


def _redact(text: str) -> str:
    for name in _SECRET_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            text = text.replace(value, "***")
    return redact_invariantql(text)


def _json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        prefix = value[:32].hex()
        suffix = "..." if len(value) > 32 else ""
        return f"<bytes length={len(value)} hex={prefix}{suffix}>"
    return str(value)


def _exercise(source: Any, *, label: str, rows: int, batch_size: int) -> None:
    """Plan and execute a fixed-name, bounded SELECT through DuckDB."""

    print(f"\n=== {label} ===")
    with iql.Context() as context:
        context.register_source(source)
        # Source names below are constants owned by this script, not external
        # identifiers, so this also safely exercises the SQL frontend.
        query = context.sql(f"SELECT * FROM {source.name} LIMIT {rows}")
        execution_plan = query.execution_plan(engine="duckdb")

        print("Logical schema:")
        print(json.dumps(execution_plan.output_schema.to_dict(), indent=2))
        print("Execution plan:")
        print(execution_plan.explain)

        # LIMIT is in the logical plan and max_rows is a second local guard.
        with query.execute(engine="duckdb", batch_size=batch_size) as result:
            records = result.rows(max_rows=rows)

        print(f"Sample rows ({len(records)}):")
        print(json.dumps(records, indent=2, ensure_ascii=False, default=_json_default))


def run_postgres(*, rows: int, batch_size: int) -> None:
    """Connect using a libpq DSN, then let PostgresSource stream the table."""

    dsn = _required("INVARIANTQL_POSTGRES_DSN")
    table = _required("INVARIANTQL_POSTGRES_TABLE")
    schema = _optional("INVARIANTQL_POSTGRES_SCHEMA") or "public"
    timeout = _integer("INVARIANTQL_POSTGRES_CONNECT_TIMEOUT", 10, maximum=600)

    try:
        import psycopg
    except ImportError:
        raise ConfigurationError(
            "PostgreSQL support is missing; run uv sync --extra duckdb --extra postgres"
        ) from None

    connection: Any = None
    try:
        # Use a read-only PostgreSQL role. Advanced libpq/TLS settings belong in
        # the DSN (for remote hosts, prefer sslmode=verify-full + sslrootcert).
        connection = psycopg.connect(
            dsn,
            connect_timeout=timeout,
            application_name="invariantql-smoke",
        )
        info = connection.info
        host = info.host or "localhost"
        database = info.dbname or ""
        user = info.user or ""
        if not database or not user:
            raise ConfigurationError("the PostgreSQL DSN must resolve a database and user")
        print(f"PostgreSQL target: {host}:{info.port or 5432}/{database}/{schema}.{table}")
        source = iql.postgres_source(
            "pg_smoke",
            host=host,
            port=int(info.port or 5432),
            database=database,
            schema=schema,
            table=table,
            user=user,
            connection=connection,
        )
        _exercise(source, label="PostgreSQL", rows=rows, batch_size=batch_size)
    finally:
        # PostgresSource intentionally does not own an injected connection.
        if connection is not None:
            connection.close()


def run_mongodb(*, rows: int, batch_size: int) -> None:
    """Infer a collection schema and stream a bounded sample through DuckDB."""

    uri = _required("INVARIANTQL_MONGODB_URI")
    database = _required("INVARIANTQL_MONGODB_DATABASE")
    collection = _required("INVARIANTQL_MONGODB_COLLECTION")
    sample_size = _integer("INVARIANTQL_MONGODB_SAMPLE_SIZE", 100, maximum=100_000)
    print(f"MongoDB target: {database}.{collection} (URI redacted)")
    source = iql.mongodb_source(
        "mongo_smoke",
        uri=uri,
        database=database,
        collection=collection,
        sample_size=sample_size,
    )
    _exercise(source, label="MongoDB", rows=rows, batch_size=batch_size)


def _azure_credentials() -> tuple[dict[str, Any], str]:
    account_key = _optional("AZURE_STORAGE_ACCOUNT_KEY")
    sas_token = _optional("AZURE_STORAGE_SAS_TOKEN")
    connection_string = _optional("AZURE_STORAGE_CONNECTION_STRING")
    client_id = _optional("AZURE_STORAGE_CLIENT_ID")
    client_secret = _optional("AZURE_STORAGE_CLIENT_SECRET")
    tenant_id = _optional("AZURE_STORAGE_TENANT_ID")
    anon = _boolean("AZURE_STORAGE_ANON", False)

    service_principal = (client_id, client_secret, tenant_id)
    if any(service_principal) and not all(service_principal):
        raise ConfigurationError(
            "service-principal auth requires AZURE_STORAGE_CLIENT_ID, "
            "AZURE_STORAGE_CLIENT_SECRET, and AZURE_STORAGE_TENANT_ID together"
        )

    methods: list[tuple[str, dict[str, Any]]] = []
    if account_key:
        methods.append(("account key", {"account_key": account_key}))
    if sas_token:
        methods.append(("SAS token", {"sas_token": sas_token}))
    if connection_string:
        methods.append(("connection string", {"connection_string": connection_string}))
    if all(service_principal):
        methods.append(
            (
                "service principal",
                {"client_id": client_id, "client_secret": client_secret, "tenant_id": tenant_id},
            )
        )
    if anon:
        methods.append(("anonymous", {"anon": True}))
    if len(methods) > 1:
        names = ", ".join(name for name, _ in methods)
        raise ConfigurationError(f"choose exactly one Azure credential method; found {names}")
    if methods:
        return methods[0][1], methods[0][0]
    return {}, "DefaultAzureCredential/provider chain"


def run_adls_json(*, rows: int, batch_size: int) -> None:
    """Read one JSON/NDJSON object from an ADLS Gen2 container."""

    account = _required("AZURE_STORAGE_ACCOUNT_NAME")
    container = _required("INVARIANTQL_ADLS_CONTAINER")
    path = _required("INVARIANTQL_ADLS_PATH")
    root = _optional("INVARIANTQL_ADLS_ROOT") or ""
    endpoint_suffix = _optional("INVARIANTQL_ADLS_ENDPOINT_SUFFIX") or "core.windows.net"
    lines = _boolean("INVARIANTQL_JSON_LINES", True)
    compression = _optional("INVARIANTQL_JSON_COMPRESSION")
    credentials, credential_label = _azure_credentials()

    storage = iql.adls_storage(
        account,
        container,
        root=root,
        endpoint_suffix=endpoint_suffix,
        **credentials,
    )
    location = storage.resolve(path)
    print(f"ADLS object: {location.uri}")
    print(f"Azure authentication: {credential_label}")
    if not storage.exists(location):
        raise ConfigurationError(
            "ADLS object was not found; INVARIANTQL_ADLS_PATH must be one exact object path "
            "relative to the configured container/root"
        )

    source = iql.file_source(
        "adls_json_smoke",
        storage,
        path,
        iql.JsonFormat(lines=lines, compression=compression),
    )
    _exercise(source, label="ADLS JSON", rows=rows, batch_size=batch_size)


RUNNERS: dict[str, Callable[..., None]] = {
    "postgres": run_postgres,
    "mongodb": run_mongodb,
    "adls-json": run_adls_json,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bounded, read-only InvariantQL integration smoke tests.",
        epilog=(
            "Examples: smoke.py postgres | smoke.py mongodb adls-json | "
            "smoke.py --rows 1 (all targets)"
        ),
    )
    parser.add_argument(
        "targets",
        nargs="*",
        choices=TARGETS,
        help="targets to run; omit to run all three",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help=f"dotenv file (default: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument("--rows", type=int, help="sample row limit (default: env or 5)")
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Arrow/provider fetch batch size (default: env or 1024)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        loaded = load_dotenv(args.env_file)
        rows = (
            args.rows
            if args.rows is not None
            else _integer("INVARIANTQL_SMOKE_ROWS", 5, maximum=10_000)
        )
        batch_size = (
            args.batch_size
            if args.batch_size is not None
            else _integer("INVARIANTQL_SMOKE_BATCH_SIZE", 1024, maximum=1_000_000)
        )
        if rows < 1:
            raise ConfigurationError("--rows must be at least 1")
        if batch_size < 1:
            raise ConfigurationError("--batch-size must be at least 1")
    except ConfigurationError as exc:
        print(f"CONFIG ERROR: {_redact(str(exc))}", file=sys.stderr)
        return 2

    if loaded:
        print(f"Loaded configuration from {args.env_file}")
    else:
        print(f"No {args.env_file}; using process environment only")

    targets = tuple(dict.fromkeys(args.targets or TARGETS))
    passed: list[str] = []
    failed: list[str] = []
    for target in targets:
        try:
            RUNNERS[target](rows=rows, batch_size=batch_size)
        except KeyboardInterrupt:
            print("\nInterrupted", file=sys.stderr)
            return 130
        except Exception as exc:
            failed.append(target)
            code = getattr(getattr(exc, "code", None), "value", None)
            suffix = f" [{code}]" if code else ""
            print(
                f"\nFAIL {target}{suffix}: {_redact(str(exc))}",
                file=sys.stderr,
            )
        else:
            passed.append(target)
            print(f"PASS {target}")

    print(f"\nSummary: {len(passed)} passed, {len(failed)} failed")
    if failed:
        print("Failed targets: " + ", ".join(failed), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
