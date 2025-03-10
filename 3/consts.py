from enum import StrEnum
from functools import cached_property
from os import environ, getenv
from typing import Final

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"

GITHUB_ACCESS_TOKEN: Final[str] = getenv("GITHUB_ACCESS_TOKEN")
if not GITHUB_ACCESS_TOKEN:
    raise RuntimeError("GITHUB_ACCESS_TOKEN must be set")

MAX_CONCURRENT_REQUESTS: Final[int] = int(getenv("MAX_CONCURRENT_REQUESTS", 20))

# primary limits: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28
REQUESTS_PER_SECOND: Final[int] = int(getenv("REQUESTS_PER_SECOND", 100))

TOKEN_WAIT_TIME_IN_SECONDS: Final[float] = float(getenv("TOKEN_WAIT_TIME_IN_SECONDS", 0.1))
PRODUCER_POLL_INTERVAL_IN_SECONDS: Final[float] = float(getenv("PRODUCER_POLL_INTERVAL_IN_SECONDS", 2))
CLIENT_TIMEOUT_IN_SECONDS: Final[int] = int(getenv("CLIENT_TIMEOUT", 60))

TOP_REPOS_NUMBER: Final[int] = int(getenv("TOP_REPOS_NUMBER", 500))

MAX_RETRIES: Final[int] = int(getenv("MAX_RETRIES", 10))
INITIAL_RETRY_DELAY: Final[int] = int(getenv("INITIAL_RETRY_DELAY", 3))
BACKOFF_FACTOR: Final[int] = int(getenv("BACKOFF_FACTOR", 2))

try:
    CLICKHOUSE_URL = environ["CLICKHOUSE_URL"]
    CLICKHOUSE_USER = environ["CLICKHOUSE_USER"]
    CLICKHOUSE_PASSWORD = environ["CLICKHOUSE_PASSWORD"]
    CLICKHOUSE_DB = environ["CLICKHOUSE_DB"]
except KeyError as e:
    error_message = f"Missing Clickhouse environment variable: {e.args[0]}"
    raise RuntimeError(error_message)


class ClickhouseTable(StrEnum):
    REPOSITORIES = getenv("REPOSITORIES_TABLE", "repositories")
    REPOSITORIES_POSITIONS = getenv("REPOSITORIES_POSITIONS_TABLE", "repositories_positions")
    REPOSITORIES_AUTHORS_COMMITS = getenv("REPOSITORIES_AUTHORS_COMMITS", "repositories_authors_commits")

    @cached_property
    def full_name(self) -> str:
        return f"{CLICKHOUSE_DB}.{self.value}"


BATCH_SIZE: Final[int] = int(getenv("BATCH_SIZE", 100))
