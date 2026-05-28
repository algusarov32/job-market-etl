"""
Abstract base class for all data loaders.

Design notes
------------
* S — each loader writes to exactly one destination.
* O — new destinations are added by subclassing, not editing.
* L — all subclasses honour the same ``load`` / ``load_batch`` contract.
* I — interface is kept deliberately thin.
* D — this module depends only on the stdlib ABC; no concrete storage lib.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Value object
# ---------------------------------------------------------------------------


@dataclass
class LoadResult:
    destination: str
    rows_written: int = 0
    rows_failed: int = 0
    details: str = ""

    @property
    def success(self) -> bool:
        return self.rows_failed == 0


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class BaseLoader(ABC):
    """
    Contract every loader must satisfy.

    Subclasses implement ``load_batch``; the public ``load`` method chunks
    an arbitrary iterable automatically.
    """

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size

    @property
    @abstractmethod
    def destination_name(self) -> str:
        """Human-readable label, e.g. ``'minio'`` or ``'clickhouse'``."""

    @abstractmethod
    def load_batch(
        self,
        records: List[Dict[str, Any]],
        **kwargs: Any,
    ) -> LoadResult:
        """
        Write one batch of records to the destination.

        Args:
            records: Non-empty list of dicts.  Schema is caller-defined.
            **kwargs: Destination-specific options (table name, bucket, …).

        Returns:
            LoadResult describing what was written.
        """

    # ------------------------------------------------------------------
    # Shared orchestration
    # ------------------------------------------------------------------

    def load(
        self,
        records: Iterable[Dict[str, Any]],
        **kwargs: Any,
    ) -> LoadResult:
        """
        Chunk *records* and delegate to ``load_batch``.

        Args:
            records: Any iterable of dicts.
            **kwargs: Forwarded to every ``load_batch`` call.

        Returns:
            Aggregated LoadResult across all batches.
        """
        aggregate = LoadResult(destination=self.destination_name)
        batch: List[Dict[str, Any]] = []

        def _flush() -> None:
            if not batch:
                return
            result = self.load_batch(list(batch), **kwargs)
            aggregate.rows_written += result.rows_written
            aggregate.rows_failed += result.rows_failed
            batch.clear()

        for record in records:
            batch.append(record)
            if len(batch) >= self.batch_size:
                _flush()

        _flush()  # remaining records

        logger.info(
            "Load complete | destination=%s written=%d failed=%d",
            self.destination_name,
            aggregate.rows_written,
            aggregate.rows_failed,
        )
        return aggregate