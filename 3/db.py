import asyncio
from aiochclient import ChClient, ChClientError
from aiohttp import ClientError

from consts import ClickhouseTable
from models import Repository
from utils import retry


RETRYABLE_EXCEPTIONS = (ClientError, ChClientError, asyncio.TimeoutError)


@retry(retryable_exceptions=RETRYABLE_EXCEPTIONS)
async def _insert_to_repositories(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert repositories data into repositories table."""
    await clickhouse_client.execute(
        f"INSERT INTO {ClickhouseTable.REPOSITORIES} VALUES",
        [repo.for_repositories_table() for repo in repositories],
    )


@retry(retryable_exceptions=RETRYABLE_EXCEPTIONS)
async def _insert_to_repositories_positions(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert repositories data into repositories_positions table."""
    await clickhouse_client.execute(
        f"INSERT INTO {ClickhouseTable.REPOSITORIES_POSITIONS} VALUES",
        [repo.for_repositories_positions_table() for repo in repositories],
    )


@retry(retryable_exceptions=RETRYABLE_EXCEPTIONS)
async def _insert_to_repositories_authors_commits(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert repositories data into repositories_authors_commits table"""
    await clickhouse_client.execute(
        f"INSERT INTO {ClickhouseTable.REPOSITORIES_AUTHORS_COMMITS} VALUES",
        [
            author_commit for repo in repositories for author_commit in repo.for_repositories_authors_commits_table()
        ],
    )


async def insert_repos(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert a batch of repositories into ClickHouse."""
    await asyncio.gather(
        _insert_to_repositories(clickhouse_client, repositories),
        _insert_to_repositories_positions(clickhouse_client, repositories),
        _insert_to_repositories_authors_commits(clickhouse_client, repositories),
    )
