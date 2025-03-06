import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from functools import wraps
from json import JSONDecodeError
from typing import Any, Callable

import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector

from consts import (
    GITHUB_API_BASE_URL,
    GITHUB_ACCESS_TOKEN,
    MAX_CONCURRENT_REQUESTS,
    REQUESTS_PER_SECOND,
    TOKEN_WAIT_TIME_IN_SECONDS,
    CLIENT_TIMEOUT_IN_SECONDS,
    TOP_REPOS_NUMBER,
    MAX_RETRIES,
    INITIAL_RETRY_DELAY,
    BACKOFF_FACTOR,
)
from logger import logger


class TokenBucket:
    """
    Implementation of the token bucket data structure for rate limiting via Token Bucket Algorithm.
    It is not thread-safe, because the assignment does nor require thread-safety.
    In a real-world situation, it is better to check for existing solutions,
    with more thorough implementation, e.g. aiolimiter.
    """
    def __init__(self, max_tokens: int, refill_rate: float | None = None):
        self._max_tokens: int = max_tokens
        self._current_tokens: int = max_tokens
        self._refill_rate: float = refill_rate or float(max_tokens)  # tokens per second
        self._updated_at: float = time.monotonic()

    async def wait_for_token(self):
        while self._current_tokens < 1:
            self._add_new_tokens()
            await asyncio.sleep(TOKEN_WAIT_TIME_IN_SECONDS)
        self._current_tokens -= 1

    def _add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self._updated_at
        new_tokens = time_since_update * self._refill_rate
        if self._current_tokens + new_tokens >= 1:
            self._current_tokens = min(self._current_tokens + new_tokens, self._max_tokens)
            self._updated_at = now


def retry[T](
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_RETRY_DELAY,
    backoff_factor: float = BACKOFF_FACTOR,
    retryable_exceptions: tuple = (aiohttp.ClientError, asyncio.TimeoutError),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    A decorator to retry an async function with exponential backoff.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            retry_delay = initial_delay

            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)

                except retryable_exceptions as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                        raise
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= backoff_factor

        return wrapper

    return decorator


@dataclass(slots=True, frozen=True)
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass(slots=True, frozen=True)
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(
        self,
        access_token: str,
        *,
        timeout_in_seconds: int = CLIENT_TIMEOUT_IN_SECONDS,
        max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
        requests_per_second: int = REQUESTS_PER_SECOND,
    ):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            },
            timeout=ClientTimeout(total=timeout_in_seconds),
            # ssl=False is probably not suitable for production code in most cases
            # However, I use it just to not install extra dependencies
            connector=TCPConnector(limit=max_concurrent_requests, ssl=False),
        )
        self._token_bucket = TokenBucket(max_tokens=requests_per_second)

    @retry()
    async def _make_request(self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None) -> Any:
        uuid_ = uuid.uuid4()

        logger.debug(f"Waiting for sending request {uuid_}")
        await self._token_bucket.wait_for_token()

        logger.debug(f"Getting request {uuid_}")
        async with self._session.request(method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params) as response:
            if response.status in (429, 500, 502, 503, 504):
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"HTTP {response.status}",
                )

            response.raise_for_status()

            try:
                response_json = await response.json()
                logger.info(f"Successfully got request {uuid_}")
                return response_json
            except JSONDecodeError as e:
                logger.error(f"Failed to decode JSON for request {uuid_}")
                raise RuntimeError(f"Failed to decode JSON from Github API. Problems on Github side") from e

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories
        """
        # github api limits on search are much lower than on the rest of endpoints (10/min):
        # https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28
        # for simplicity let's suppose that we never need repos on position more than 1000,
        # to not be blocked by this limit
        if limit > 1000:
            raise ValueError("limit of _get_top_repositories must be less than 1000")

        per_page = 100
        total_pages = (limit + per_page - 1) // per_page

        tasks = [
            self._make_request(
                endpoint="search/repositories",
                params={
                    "q": "stars:>1",
                    "sort": "stars",
                    "order": "desc",
                    "per_page": min(per_page, limit - (page - 1) * per_page),
                    "page": page,
                },
            )
            for page in range(1, total_pages + 1)
        ]
        results = await asyncio.gather(*tasks)

        return [item for data in results for item in data["items"]][:limit]

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        since_time = (datetime.now(UTC) - timedelta(days=1)).isoformat()

        first_page = await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": since_time, "page": 1, "per_page": 100},
        )
        if not first_page:
            return []

        estimated_pages = (len(first_page) // 100) + 1
        tasks = [
            self._make_request(
                endpoint=f"repos/{owner}/{repo}/commits",
                params={"since": since_time, "page": page, "per_page": 100},
            )
            for page in range(2, estimated_pages + 1)
        ]

        remaining_pages = await asyncio.gather(*tasks)

        commits = first_page + [commit for page in remaining_pages if page for commit in page]

        return commits

    async def _fetch_repository(self, repo: dict[str, Any], position: int) -> Repository:
        """Fetch repository details including commit counts asynchronously."""
        owner = repo["owner"]["login"]
        repo_name = repo["name"]

        commits = await self._get_repository_commits(owner=owner, repo=repo_name)
        authors_commits_num_today = {}

        for commit in commits:
            author = commit.get("commit", {}).get("author", {}).get("name")
            if author:
                authors_commits_num_today[author] = authors_commits_num_today.get(author, 0) + 1

        authors_commits_list = [
            RepositoryAuthorCommitsNum(author=author, commits_num=commits_num)
            for author, commits_num in authors_commits_num_today.items()
        ]

        return Repository(
            name=repo_name,
            owner=owner,
            position=position,
            stars=repo["stargazers_count"],
            watchers=repo["watchers_count"],
            forks=repo["forks_count"],
            language=repo["language"] or "N/A",
            authors_commits_num_today=authors_commits_list,
        )

    async def get_repositories(self) -> list[Repository]:
        """Fetch top repositories and their commit data concurrently."""
        top_repositories = await self._get_top_repositories(limit=TOP_REPOS_NUMBER)
        tasks = [self._fetch_repository(repo, i) for i, repo in enumerate(top_repositories, start=1)]
        return await asyncio.gather(*tasks)

    async def close(self):
        await self._session.close()


async def main():
    scraper = GithubReposScrapper(access_token=GITHUB_ACCESS_TOKEN)

    try:
        repositories = await scraper.get_repositories()
        for i, repo in enumerate(repositories, start=1):
            print(f"{i:04}. {repo}", flush=True)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
