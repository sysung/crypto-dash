from fastapi import HTTPException, status
import clickhouse_connect

from app import config


def get_db_client() -> clickhouse_connect.driver.Client:
    """
    Safely connects to ClickHouse without calling sys.exit(1) on failure.
    Raises HTTPException (503 Service Unavailable) with structured details if unreachable.
    """
    try:
        return clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            connect_timeout=3  # fail fast
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Database Connectivity Failure",
                "details": f"ClickHouse database is currently unreachable on {config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}. Underline error: {str(e)}"
            }
        )
