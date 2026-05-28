"""
Greenplum (PostgreSQL-compatible) loader.

Uses temp table + INSERT ... SELECT WHERE NOT EXISTS for bulk inserts.
"""

import json
import logging
from typing import Any, Dict, List

import psycopg2.extras

from src.loaders.base_loader import BaseLoader, LoadResult
from src.utils.gp_connection import gp_connection

logger = logging.getLogger(__name__)


class GreenplumLoaderError(Exception):
    """Raised when a Greenplum write operation fails."""


class GreenplumLoader(BaseLoader):
    """
    Inserts records into a Greenplum staging table.

    The raw vacancy JSON is stored in a single ``jsonb`` column so no schema
    changes are required when source APIs add new fields.

    Deduplication strategy: records are first loaded into a temporary table,
    then inserted into the target table with WHERE NOT EXISTS to skip
    existing (vacancy_id, source) pairs.

    Usage::

        loader = GreenplumLoader()
        loader.load(records=vacancies, table="staging.vacancies_raw", source="hh.ru")
    """

    @property
    def destination_name(self) -> str:
        return "greenplum"

    def load_batch(
        self,
        records: List[Dict[str, Any]],
        table: str = "staging.vacancies_raw",
        source: str = "unknown",
        **_kwargs: Any,
    ) -> LoadResult:
        """
        Insert *records* into *table*, skipping existing (vacancy_id, source) pairs.

        Args:
            records: Each dict must contain an ``"id"`` field.
            table:   Fully-qualified table name (schema.table).
            source:  Source identifier stored alongside the raw JSON.

        Returns:
            LoadResult with rows_written count.
        """
        result = LoadResult(destination=self.destination_name)

        if not records:
            return result

        rows = [
            (str(r.get("id", "")), json.dumps(r, ensure_ascii=False), source)
            for r in records
        ]

        try:
            with gp_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TEMP TABLE _temp_load (
                            vacancy_id TEXT,
                            raw_json   JSONB,
                            source     TEXT
                        ) ON COMMIT DROP
                        DISTRIBUTED BY (vacancy_id)
                        """
                    )

                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO _temp_load (vacancy_id, raw_json, source) VALUES %s",
                        rows,
                        page_size=500,
                    )

                    cur.execute(
                        f"""
                        INSERT INTO {table} (vacancy_id, raw_json, source)
                        SELECT t.vacancy_id, t.raw_json, t.source
                        FROM _temp_load t
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {table} v
                            WHERE v.vacancy_id = t.vacancy_id
                              AND v.source     = t.source
                        )
                        """
                    )

                result.rows_written = cur.rowcount
                conn.commit()

            logger.info(
                "Inserted %d rows into %s (source=%s)",
                result.rows_written,
                table,
                source,
            )
        except Exception as exc:
            result.rows_failed = len(rows)
            result.details = str(exc)
            logger.error("load_batch to %s failed: %s", table, exc)

        return result
    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def execute_script(self, sql_path: str) -> None:
        """
        Run a SQL script file against Greenplum.

        Args:
            sql_path: Filesystem path to the ``.sql`` file.

        Raises:
            GreenplumLoaderError: On any database error.
        """
        with open(sql_path, "r", encoding="utf-8") as fh:
            sql = fh.read()
        try:
            with gp_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
            logger.info("Script executed: %s", sql_path)
        except Exception as exc:
            raise GreenplumLoaderError(
                f"Script '{sql_path}' failed: {exc}"
            ) from exc

    def table_count(self, table: str) -> int:
        """Return the number of rows in *table* (useful in tests/DAGs)."""
        with gp_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                row = cur.fetchone()
                return int(row[0]) if row else 0
