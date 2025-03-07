import asyncio
from asyncio import Future, Queue
from contextlib import asynccontextmanager
from typing import Any

from aiochclient import ChClient

from consts import (
    BATCH_SIZE,
    GITHUB_ACCESS_TOKEN,
    PRODUCER_POLL_INTERVAL_IN_SECONDS,
    TOP_REPOS_NUMBER,
)
from db import create_clickhouse_client, insert_repos
from logger import app_logger, processor_logger, worker_logger
from scraper import GithubReposScraper, create_github_scraper

_tasks_number_to_fetch: int | None = None
_processed_tasks_number: int | None = None


async def fetch_and_enqueue(repo: dict[str, Any], index: int, queue: Queue, scraper: GithubReposScraper) -> None:
    """Fetch a repository and put it into the queue."""
    fetched_repo = await scraper.fetch_repository(repo, index)
    await queue.put(fetched_repo)
    worker_logger.debug(f"Added repository {fetched_repo} to queue")


async def scraper_worker(queue: Queue, meta_future: Future, scraper: GithubReposScraper) -> None:
    worker_logger.info("Starting scraper worker")
    top_repositories = await scraper.get_top_repositories(limit=TOP_REPOS_NUMBER)

    repos_number_to_fetch = len(top_repositories)
    worker_logger.info(f"Fetched {repos_number_to_fetch} top repositories")
    meta_future.set_result(repos_number_to_fetch)

    fetch_tasks = []
    for i, repo in enumerate(top_repositories, start=1):
        task = asyncio.create_task(fetch_and_enqueue(repo, i, queue, scraper))
        fetch_tasks.append(task)

    await asyncio.gather(*fetch_tasks)
    worker_logger.info(f"Finished scraper worker. Total repositories added to queue: {len(top_repositories)}")


async def batch_processor(queue: Queue, meta_future: asyncio.Future, clickhouse_client: ChClient) -> None:
    """Processor handling repositories from queue and saving it into ClickHouse in batches."""
    processor_logger.info("Starting batch processor")
    batch = []
    batched_count = 0
    processed_count = 0

    tasks_number = await meta_future
    processor_logger.info(f"Total tasks to process: {tasks_number}")

    while batched_count < tasks_number:
        result = await queue.get()

        batch.append(result)
        batched_count += 1
        processor_logger.debug(f"Added batch result: {result}. Batched count: {batched_count}")

        if (len(batch) >= BATCH_SIZE) or (batched_count == tasks_number):
            processor_logger.info(f"Processing batch of size {len(batch)}")
            await insert_repos(clickhouse_client=clickhouse_client, repositories=batch)
            processed_count += len(batch)
            batch.clear()

        queue.task_done()

    processor_logger.info(f"Batch processor exiting. Total repositories processed: {processed_count}")
    if processed_count == tasks_number:
        processor_logger.info("All tasks processed successfully.")
    else:
        processor_logger.warning(f"Expected {tasks_number} tasks, but processed {processed_count} tasks.")


@asynccontextmanager
async def create_queue():
    app_logger.info("Creating queue")
    queue = Queue()
    app_logger.info("Queue created")
    try:
        yield queue
    finally:
        app_logger.info("Draining the queue before exiting...")
        while not queue.empty():
            await queue.get()
            queue.task_done()
        app_logger.info(f"Queue is empty, exiting...")


async def main():
    meta_future = asyncio.Future()
    try:
        async with (
            create_queue() as queue,
            create_github_scraper(access_token=GITHUB_ACCESS_TOKEN) as scraper,
            create_clickhouse_client() as db_client,
        ):
            scraper_task = asyncio.create_task(
                scraper_worker(queue=queue, meta_future=meta_future, scraper=scraper),
            )
            processor_task = asyncio.create_task(
                batch_processor(queue=queue, meta_future=meta_future, clickhouse_client=db_client),
            )

            await asyncio.gather(scraper_task, processor_task)

    except Exception as e:
        app_logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
