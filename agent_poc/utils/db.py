import os
import sys
import clickhouse_connect


def get_clickhouse_client() -> clickhouse_connect.driver.Client:
    """
    Initializes and returns a connection to the ClickHouse database.
    Configuration values are fetched from environment variables with safe defaults.
    """
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "password123")

    try:
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password
        )
    except Exception as e:
        print(f"❌ ClickHouse Database Connection Failed: {e}", file=sys.stderr)
        sys.exit(1)
