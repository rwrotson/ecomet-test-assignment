from aiochclient import ChClient

from consts import CLICKHOUSE_URL, CLICKHOUSE_PASSWORD, CLICKHOUSE_USER, CLICKHOUSE_DB, BATCH_SIZE
from logger import logger
from models import Repository


class ClickhouseClient:
    def __init__(
        self,
        *,
        url: str = CLICKHOUSE_URL,
        db_name: str = CLICKHOUSE_DB,
        user: str = CLICKHOUSE_USER,
        password: str = CLICKHOUSE_PASSWORD,
    ):
        self._clickhouse_client = ChClient(
            url=url,
            user=user,
            password=password,
            database=db_name,
        )
        logger.info("Clickhouse client created")

    def _prepare_repos(self, repositories: list[Repository]) -> None:
        repos_data = [repo.for_repositories_table() for repo in repositories]
        repos_positions_data = [repo.for_repositories_positions_table() for repo in repositories]
        repos_authors_commits_data = [
            author_commit for repo in repositories for author_commit in repo.for_repositories_authors_commits_table()
        ]

    def _insert(self, rep):

    async def _insert_repos(self, repositories: list[Repository]) -> None:
        """Insert a batch of repositories into ClickHouse."""

        await self._clickhouse_client.execute(
            "INSERT INTO test.repositories VALUES",
            repos_data,
        )
        await self._clickhouse_client.execute(
            "INSERT INTO test.repositories_positions VALUES",
            repos_positions_data,
        )
        await self._clickhouse_client.execute(
            "INSERT INTO test.repositories_authors_commits VALUES",
            repos_authors_commits_data,
        )