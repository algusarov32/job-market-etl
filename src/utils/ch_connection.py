"""
ClickHouse connection helper.

Usage:
    with ch_client() as client:
        client.execute(...)
"""

import logging
from contextlib import contextmanager
from typing import Generator

from clickhouse_driver import Client

from src.utils.config import clickhouse as ch_cfg

logger = logging.getLogger(__name__)


@contextmanager
def ch_client() -> Generator[Client, None, None]:
    """
    Yield a ClickHouse Driver client.

    The clickhouse_driver.Client is not a context-manager itself, so we wrap
    it here for consistent resource handling across the codebase.

    Yields:
        clickhouse_driver.Client connected to the configured instance.

    Raises:
        Exception: Propagates any connection-level error from the driver.
    """
    client: Client | None = None
    try:
        client = Client(
            host=ch_cfg.host,
            port=ch_cfg.native_port,
            database=ch_cfg.database,
            user=ch_cfg.user,
            password=ch_cfg.password,
            connect_timeout=10,
            send_receive_timeout=300,
        )
        logger.debug(
            "ClickHouse connection opened: %s:%s/%s",
            ch_cfg.host,
            ch_cfg.native_port,
            ch_cfg.database,
        )
        yield client
    except Exception as exc:
        logger.error("Failed to connect to ClickHouse: %s", exc)
        raise
    finally:
        if client is not None:
            client.disconnect()
            logger.debug("ClickHouse connection closed")