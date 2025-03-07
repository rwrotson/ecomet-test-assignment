import asyncio
from asyncio import Queue
from contextlib import asynccontextmanager

from aiochclient import ChClient

from consts import GITHUB_ACCESS_TOKEN, CLICKHOUSE_URL, CLICKHOUSE_PASSWORD, CLICKHOUSE_USER, CLICKHOUSE_DB, BATCH_SIZE, TOP_REPOS_NUMBER, ASYNCIO_POLL_TIME_IN_SECONDS
from db import insert_repos
from logger import logger
from scraper import GithubReposScraper


async def scraper_worker(queue: Queue, scraper: GithubReposScraper) -> None:
    top_repositories = await scraper.get_top_repositories(limit=TOP_REPOS_NUMBER)

    tasks = []
    for i, repo in enumerate(top_repositories, start=1):
        task = asyncio.create_task(scraper.fetch_repository(repo, i))
        tasks.append(task)

    for task in asyncio.as_completed(tasks):
        fetched_repo = await task
        await queue.put(fetched_repo)

    await queue.put(None)  # Sentinel value to signal that scraper is finished its work


async def batch_processor(queue: Queue, clickhouse_client: ChClient) -> None:
    while True:
        if (queue.qsize() >= BATCH_SIZE) or (queue.qsize() > 0 and (queue.qsize() < BATCH_SIZE) and queue.empty()):
            batch_size = min(queue.qsize(), BATCH_SIZE)
            batch = []
            for _ in range(batch_size):
                result = await queue.get()
                if result is None:  # If sentinel value, process the remaining items and exit
                    if batch:
                        await insert_repos(clickhouse_client=clickhouse_client, repositories=batch)
                    return
                batch.append(result)

            await insert_repos(clickhouse_client=clickhouse_client, repositories=batch)

            for _ in range(batch_size):
                queue.task_done()

        elif queue.empty():
            break  # Exit if the queue is empty and no more items are expected

        else:
            await asyncio.sleep(ASYNCIO_POLL_TIME_IN_SECONDS)


@asynccontextmanager
async def create_queue():
    logger.info("Creating queue")
    queue = Queue()
    logger.info("Queue created")
    try:
        yield queue
    finally:
        logger.info("Draining the queue before exiting...")
        while not queue.empty():
            await queue.get()
            queue.task_done()
        logger.info(f"Queue is empty, exiting...")


@asynccontextmanager
async def create_github_scraper(access_token: str):
    logger.info("Initializing GitHub scraper...")
    scraper = GithubReposScraper(access_token=access_token)
    logger.info("GitHub scraper initialized.")
    try:
        yield scraper
    finally:
        logger.info("Finishing GitHub scraper...")
        await scraper.close()
        logger.info("GitHub scraper finished.")


@asynccontextmanager
async def create_clickhouse_client():
    logger.info("Initializing ClickHouse client...")
    db_client = ChClient(
        url=CLICKHOUSE_URL,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
    logger.info("ClickHouse client initialized.")
    try:
        yield db_client
    finally:
        logger.info("Finishing ClickHouse client...")
        await db_client.close()
        logger.info("ClickHouse client finished.")


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
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
