"""
Abstract base class for all vacancy extractors.

Design principles applied
--------------------------
S — Each extractor has one job: pull data from one source.
O — New sources are added by subclassing, not by modifying this file.
L — Any BaseExtractor subclass can be used wherever BaseExtractor is expected.
I — Interfaces are thin; only what every extractor truly needs is abstract.
D — This module depends on abstractions (ABC), not on concrete HTTP or DB libs.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Value object — carries extraction run statistics
# ---------------------------------------------------------------------------


@dataclass
class ExtractionStats:
    source: str
    extracted_count: int = 0
    error_count: int = 0
    duration_seconds: float = 0.0
    started_at: Optional[datetime] = None

    @property
    def vacancies_per_second(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.extracted_count / self.duration_seconds

    def as_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "extracted_count": self.extracted_count,
            "error_count": self.error_count,
            "duration_seconds": round(self.duration_seconds, 3),
            "vacancies_per_second": round(self.vacancies_per_second, 2),
            "started_at": self.started_at.isoformat() if self.started_at else None,
        }


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class BaseExtractor(ABC):
    """
    Contract every extractor must satisfy.

    Concrete extractors override ``extract_vacancies`` and ``source_name``.
    The public entry-point is ``run()`` which adds timing and logging for free.
    """

    def __init__(self, rate_limit_delay: float = 0.25) -> None:
        self.rate_limit_delay = rate_limit_delay
        self._stats = ExtractionStats(source=self.source_name)
        logger.info("Initialized %s", self.__class__.__name__)

    # ------------------------------------------------------------------
    # Abstract interface — must be implemented by every subclass
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable identifier, e.g. ``'hh.ru'``."""

    @abstractmethod
    def extract_vacancies(
        self,
        search_query: str,
        limit: int = 100,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Fetch vacancies from the source.

        Args:
            search_query: Free-text query forwarded to the source API.
            limit: Upper bound on the number of returned items.
            **kwargs: Source-specific parameters.

        Returns:
            List of raw vacancy dicts.  Schema is source-specific; callers
            should pass the result through a transformer before storage.
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def normalize_vacancy(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Wrap a raw vacancy with extraction metadata.

        Override in subclasses that want richer normalization while still
        calling ``super().normalize_vacancy(raw)`` to keep the base fields.
        """
        return {
            "source": self.source_name,
            "extracted_at": datetime.utcnow().isoformat(),
            "raw_data": raw,
        }

    def _apply_rate_limit(self) -> None:
        """Sleep between requests to avoid hitting source-side throttling."""
        if self.rate_limit_delay > 0:
            time.sleep(self.rate_limit_delay)

    # ------------------------------------------------------------------
    # Public orchestration
    # ------------------------------------------------------------------

    def run(
        self,
        search_query: str,
        limit: int = 100,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Execute extraction, emit structured logs, and return vacancies.

        This is the recommended entry-point for DAGs and scripts.
        """
        self._stats = ExtractionStats(
            source=self.source_name,
            started_at=datetime.utcnow(),
        )
        start = time.monotonic()

        logger.info(
            "Extraction started | source=%s query='%s' limit=%d",
            self.source_name,
            search_query,
            limit,
        )

        try:
            vacancies = self.extract_vacancies(
                search_query=search_query,
                limit=limit,
                **kwargs,
            )
            self._stats.extracted_count = len(vacancies)

        except Exception as exc:
            self._stats.error_count += 1
            logger.error("Extraction failed | source=%s error=%s", self.source_name, exc)
            raise

        finally:
            self._stats.duration_seconds = time.monotonic() - start
            logger.info(
                "Extraction finished | %s",
                " ".join(f"{k}={v}" for k, v in self._stats.as_dict().items()),
            )

        return vacancies

    @property
    def stats(self) -> ExtractionStats:
        """Read-only access to stats from the last ``run()`` call."""
        return self._stats