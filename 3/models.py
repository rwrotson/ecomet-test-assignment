from dataclasses import dataclass
from datetime import datetime, date, UTC
from typing import NamedTuple


class RepositoriesDataItem(NamedTuple):
    name: str
    owner: str
    stars: int
    watchers: int
    forks: int
    language: str
    updated: datetime


class RepositoriesAuthorsCommitsItem(NamedTuple):
    date: date
    repo: str
    author: str
    commits_num: int


class RepositoriesPositionsItem(NamedTuple):
    date: date
    repo: str
    position: int


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

    def for_repositories_table(self, updated: datetime | None = None) -> RepositoriesDataItem:
        return RepositoriesDataItem(
            name=self.name,
            owner=self.owner,
            stars=self.stars,
            watchers=self.watchers,
            forks=self.forks,
            language=self.language,
            updated=updated or datetime.now(UTC),
        )

    def for_repositories_authors_commits_table(self, date_: date | None = None) -> list[RepositoriesAuthorsCommitsItem]:
        return [
            RepositoriesAuthorsCommitsItem(
                date=date_ or datetime.now(UTC).date(),
                repo=self.name,
                author=commit.author,
                commits_num=commit.commits_num,
            ) for commit in self.authors_commits_num_today
        ]

    def for_repositories_positions_table(self, date_: date | None = None) -> RepositoriesPositionsItem:
        return RepositoriesPositionsItem(
            date=date_,
            repo=self.name,
            position=self.position,
        )
