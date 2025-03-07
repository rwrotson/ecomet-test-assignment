import asyncio
from asyncio import Queue
from contextlib import asynccontextmanager

from aiochclient import ChClient

from consts import GITHUB_ACCESS_TOKEN, BATCH_SIZE, TOP_REPOS_NUMBER, ASYNCIO_POLL_TIME_IN_SECONDS
from db import insert_repos, create_clickhouse_client
from logger import app_logger, worker_logger, processor_logger
from scraper import GithubReposScraper, create_github_scraper


async def scraper_worker(queue: Queue, scraper: GithubReposScraper) -> None:
    worker_logger.info("Starting scraper worker")
    top_repositories = await scraper.get_top_repositories(limit=TOP_REPOS_NUMBER)

    for i, repo in enumerate(top_repositories, start=1):
        fetched_repo = await scraper.fetch_repository(repo, i)
        await queue.put(fetched_repo)
        worker_logger.debug(f"Added repository {fetched_repo} to queue")

    await queue.put(None)
    worker_logger.info("Finished scraper worker")


async def batch_processor(queue: Queue, clickhouse_client: ChClient) -> None:
    """Processor handling repositories from queue and saving it into ClickHouse in batches."""
    processor_logger.info("Starting batch processor")
    while True:
        if queue.qsize() >= BATCH_SIZE or (queue.qsize() > 0 and queue.empty()):
            processor_logger.info("Batch processor activated")
            batch_size = min(queue.qsize(), BATCH_SIZE)
            batch = []
            processor_logger.debug(f"Preparing batch size: {batch_size}")
            for _ in range(batch_size):
                result = await queue.get()
                if result is None:  # If sentinel value, process the remaining items and exit
                    if batch:
                        await insert_repos(clickhouse_client=clickhouse_client, repositories=batch)
                    processor_logger.info("Batch processor exiting (sentinel value received)")
                    return
                batch.append(result)
                processor_logger.debug(f"Added batch result: {result}")

            await insert_repos(clickhouse_client=clickhouse_client, repositories=batch)

            for _ in range(batch_size):
                queue.task_done()
            processor_logger.debug(f"Finished processing batch")

        elif queue.empty():
            processor_logger.debug("Queue is empty, waiting for items...")
            await asyncio.sleep(ASYNCIO_POLL_TIME_IN_SECONDS)

        else:
            await asyncio.sleep(ASYNCIO_POLL_TIME_IN_SECONDS)
            processor_logger.debug("Batch processor sleeping")


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
    try:
        async with (
            create_queue() as queue,
            create_github_scraper(access_token=GITHUB_ACCESS_TOKEN) as scraper,
            create_clickhouse_client() as db_client,
        ):
            scraper_task = asyncio.create_task(scraper_worker(queue, scraper))
            processor_task = asyncio.create_task(batch_processor(queue, db_client))

            await asyncio.gather(scraper_task, processor_task)

    except Exception as e:
        app_logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
