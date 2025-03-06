from os import getenv
from typing import Final


GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"

GITHUB_ACCESS_TOKEN: Final[str] = getenv("GITHUB_ACCESS_TOKEN")
if not GITHUB_ACCESS_TOKEN:
    raise RuntimeError("GITHUB_ACCESS_TOKEN must be set")

MAX_CONCURRENT_REQUESTS: Final[int] = int(getenv("MAX_CONCURRENT_REQUESTS", 20))

# primary limits:
# https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28
REQUESTS_PER_SECOND: Final[int] = int(getenv("REQUESTS_PER_SECOND", 100))

TOKEN_WAIT_TIME_IN_SECONDS: Final[float] = float(getenv("TOKEN_WAIT_TIME_IN_SECONDS", 0.1))
CLIENT_TIMEOUT_IN_SECONDS: Final[int] = int(getenv("CLIENT_TIMEOUT", 60))

TOP_REPOS_NUMBER: Final[int] = int(getenv("TOP_REPOS_NUMBER", 500))

MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1
BACKOFF_FACTOR = 2
