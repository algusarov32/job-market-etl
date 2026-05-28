"""
Greenplum / PostgreSQL connection helper.

Usage:
    with gp_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(...)
"""

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extensions

from src.utils.config import greenplum as gp_cfg

logger = logging.getLogger(__name__)


@contextmanager
def gp_connection(
    autocommit: bool = False,
) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Yield an open psycopg2 connection and close it when the block exits.

    Args:
        autocommit: Set connection-level autocommit.

    Yields:
        psycopg2 connection object.

    Raises:
        psycopg2.OperationalError: When the host is unreachable or credentials
            are wrong.
    """
    conn: psycopg2.extensions.connection | None = None
    try:
        conn = psycopg2.connect(
            host=gp_cfg.host,
            port=gp_cfg.port,
            dbname=gp_cfg.database,
            user=gp_cfg.user,
            password=gp_cfg.password,
            connect_timeout=10,
        )
        conn.autocommit = autocommit
        logger.debug(
            "Greenplum connection opened: %s:%s/%s",
            gp_cfg.host,
            gp_cfg.port,
            gp_cfg.database,
        )
        yield conn
    except psycopg2.OperationalError as exc:
        logger.error("Failed to connect to Greenplum: %s", exc)
        raise
    finally:
        if conn is not None and not conn.closed:
            conn.close()
            logger.debug("Greenplum connection closed")