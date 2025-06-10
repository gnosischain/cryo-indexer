"""Simple migrations runner."""
import sys
from loguru import logger

from ..config import settings
from .clickhouse_manager import ClickHouseManager
from ..core.utils import setup_logging


def run_migrations():
    """Run database migrations."""
    setup_logging("INFO", "/app/logs")
    
    logger.info("Running database migrations...")
    
    # Create ClickHouse manager
    clickhouse = ClickHouseManager(
        host=settings.clickhouse_host,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        port=settings.clickhouse_port,
        secure=settings.clickhouse_secure
    )
    
    # Run migrations
    success = clickhouse.run_migrations(settings.migrations_dir)
    
    if success:
        logger.info("Migrations completed successfully")
        sys.exit(0)
    else:
        logger.error("Migrations failed")
        sys.exit(1)


if __name__ == "__main__":
    run_migrations()