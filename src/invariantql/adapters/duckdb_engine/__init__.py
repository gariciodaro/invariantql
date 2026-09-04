"""DuckDB local execution engine adapter."""

from invariantql.adapters.duckdb_engine.engine import DuckDBEngine
from invariantql.adapters.duckdb_engine.result import DEFAULT_MATERIALIZE_ROWS, LocalResult

__all__ = ["DEFAULT_MATERIALIZE_ROWS", "DuckDBEngine", "LocalResult"]
