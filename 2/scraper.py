import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from json import JSONDecodeError
from typing import Any

import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from consts import (
    CLIENT_TIMEOUT_IN_SECONDS,
    GITHUB_API_BASE_URL,
    MAX_CONCURRENT_REQUESTS,
    REQUESTS_PER_SECOND,
    TOP_REPOS_NUMBER,
)
from logger import scraper_logger
from utils import TokenBucket, retry


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


class GithubReposScraper:
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
        scraper_logger.info(f"Created Github Repos Scraper with access_token")

    @retry()
    async def _make_request(self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None) -> Any:
        uuid_ = uuid.uuid4()

        scraper_logger.debug(f"Waiting for sending request {uuid_}")
        await self._token_bucket.wait_for_token()

        scraper_logger.debug(f"Getting request {uuid_}")
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
                scraper_logger.info(f"Successfully got request {uuid_}")
                return response_json
            except JSONDecodeError as e:
                scraper_logger.error(f"Failed to decode JSON for request {uuid_}")
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
            if author := commit.get("commit", {}).get("author", {}).get("name"):
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
