import asyncio
from contextlib import asynccontextmanager

from aiochclient import ChClient, ChClientError
from aiohttp import ClientError

from consts import (
    CLICKHOUSE_DB,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
    ClickhouseTable,
)
from logger import db_logger
from models import Repository
from utils import retry


@asynccontextmanager
async def create_clickhouse_client():
    db_logger.info("Initializing ClickHouse client...")
    db_client = ChClient(
        url=CLICKHOUSE_URL,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
    db_logger.info("ClickHouse client initialized.")
    try:
        yield db_client
    finally:
        db_logger.info("Finishing ClickHouse client...")
        await db_client.close()
        db_logger.info("ClickHouse client finished.")


RETRYABLE_EXCEPTIONS = (ClientError, ChClientError, asyncio.TimeoutError)


@retry(retryable_exceptions=RETRYABLE_EXCEPTIONS)
async def _insert_to_repositories(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert repositories data into repositories table."""
    db_logger.info(f"Inserting {len(repositories)} repositories")
    await clickhouse_client.execute(
        f"INSERT INTO {ClickhouseTable.REPOSITORIES.full_name} VALUES",
        *[tuple(repo.for_repositories_table()) for repo in repositories],
    )
    db_logger.info(f"Inserted {len(repositories)} repositories")


@retry(retryable_exceptions=RETRYABLE_EXCEPTIONS)
async def _insert_to_repositories_positions(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert repositories data into repositories_positions table."""
    db_logger.info(f"Inserted {len(repositories)} repositories positions")
    await clickhouse_client.execute(
        f"INSERT INTO {ClickhouseTable.REPOSITORIES_POSITIONS.full_name} VALUES",
        *[tuple(repo.for_repositories_positions_table()) for repo in repositories],
    )
    db_logger.info(f"Inserted {len(repositories)} repositories positions")


@retry(retryable_exceptions=RETRYABLE_EXCEPTIONS)
async def _insert_to_repositories_authors_commits(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert repositories data into repositories_authors_commits table"""
    db_logger.info(f"Inserting {len(repositories)} repositories authors commits...")
    await clickhouse_client.execute(
        f"INSERT INTO {ClickhouseTable.REPOSITORIES_AUTHORS_COMMITS.full_name} VALUES",
        *[
            tuple(author_commit)
            for repo in repositories
            for author_commit in repo.for_repositories_authors_commits_table()
        ],
    )
    db_logger.info(f"Inserted {len(repositories)} repositories authors commits")


async def insert_repos(clickhouse_client: ChClient, repositories: list[Repository]) -> None:
    """Insert a batch of repositories into ClickHouse."""
    db_logger.info(f"Inserting {len(repositories)} repositories data into ClickHouse...")
    await asyncio.gather(
        _insert_to_repositories(clickhouse_client, repositories),
        _insert_to_repositories_positions(clickhouse_client, repositories),
        _insert_to_repositories_authors_commits(clickhouse_client, repositories),
    )
    db_logger.info(f"Inserted {len(repositories)} repositories data into ClickHouse.")
