"""
ClickHouse loader.

Uses the native protocol (clickhouse-driver) for bulk inserts — much faster
than HTTP INSERT for large batches.

Target tables: facts.skills_stats, facts.company_stats, facts.market_dynamics.
Data is read from Greenplum marts.* and loaded as-is into ClickHouse.
Grafana aggregates on the fly.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Tuple

from src.loaders.base_loader import BaseLoader, LoadResult
from src.utils.ch_connection import ch_client

logger = logging.getLogger(__name__)


class ClickHouseLoaderError(Exception):
    """Raised when a ClickHouse write fails."""


class ClickHouseLoader(BaseLoader):
    """
    Inserts pre-aggregated mart data into ClickHouse.

    Expected tables match Greenplum marts:
        facts.skills_stats     ← marts.skills_stats
        facts.company_stats    ← marts.company_stats
        facts.market_dynamics  ← marts.market_dynamics

    Usage::

        loader = ClickHouseLoader()
        loader.load(records=rows, table="facts.skills_stats")
    """

    _SKILLS_COLUMNS: Tuple[str, ...] = (
        "stat_date",
        "skill_name",
        "city_name",
        "vacancy_count",
        "avg_salary_from",
        "avg_salary_to",
        "median_salary",
        "min_salary",
        "max_salary",
        "junior_count",
        "middle_count",
        "senior_count",
        "remote_count",
        "office_count",
    )

    _COMPANY_COLUMNS: Tuple[str, ...] = (
        "stat_date",
        "company_name",
        "active_vacancies",
        "avg_salary",
        "max_salary",
        "cities_count",
    )

    _MARKET_COLUMNS: Tuple[str, ...] = (
        "stat_date",
        "city_name",
        "total_vacancies",
        "active_companies",
        "avg_salary",
        "median_salary",
        "remote_vacancies",
        "office_vacancies",
        "junior_vacancies",
        "middle_vacancies",
        "senior_vacancies",
    )

    _COLUMNS_MAP = {
        "facts.skills_stats": _SKILLS_COLUMNS,
        "facts.company_stats": _COMPANY_COLUMNS,
        "facts.market_dynamics": _MARKET_COLUMNS,
    }

    @property
    def destination_name(self) -> str:
        return "clickhouse"

    # ------------------------------------------------------------------
    # BaseLoader contract
    # ------------------------------------------------------------------

    def load_batch(
        self,
        records: List[Dict[str, Any]],
        table: str = "facts.skills_stats",
        **_kwargs: Any,
    ) -> LoadResult:
        """
        Insert *records* into *table*.

        Args:
            records: List of dicts whose keys match the table columns.
            table:   Target table (schema.table).

        Returns:
            LoadResult with rows_written count.
        """
        result = LoadResult(destination=self.destination_name)
        if not records:
            return result

        columns = self._COLUMNS_MAP.get(table)
        if columns is None:
            raise ClickHouseLoaderError(f"Unknown table: {table}")

        rows = [tuple(r.get(col) for col in columns) for r in records]
        col_list = ", ".join(columns)

        try:
            with ch_client() as client:
                client.execute(
                    f"INSERT INTO {table} ({col_list}) VALUES",  # noqa: S608
                    rows,
                )
            result.rows_written = len(rows)
            logger.info("Inserted %d rows into '%s'", result.rows_written, table)
        except Exception as exc:
            result.rows_failed = len(rows)
            result.details = str(exc)
            logger.error("load_batch to %s failed: %s", table, exc)

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def ensure_table(self, ddl: str) -> None:
        """Execute a CREATE TABLE IF NOT EXISTS statement."""
        with ch_client() as client:
            client.execute(ddl)
        logger.info("ensure_table executed successfully")

    def drop_partition_for_date(self, table: str, partition_date: date) -> None:
        """
        Drop the MergeTree partition that contains *partition_date*.

        Used before a reload to avoid duplicates when a DAG re-runs.
        """
        partition_id = partition_date.strftime("%Y%m")
        with ch_client() as client:
            client.execute(
                f"ALTER TABLE {table} DROP PARTITION {partition_id}"  # noqa: S608
            )
        logger.info("Dropped partition %s from %s", partition_id, table)

    def table_count(self, table: str) -> int:
        """Return the number of rows in *table*."""
        with ch_client() as client:
            rows = client.execute(f"SELECT COUNT() FROM {table}")  # noqa: S608
            return int(rows[0][0]) if rows else 0