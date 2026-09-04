"""Storage adapters. Provider factories live in their own modules and import lazily."""

from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.adapters.storage.local import LocalStorage

__all__ = ["FsspecStorage", "LocalStorage"]
