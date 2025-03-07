import asyncio

from consts import GITHUB_ACCESS_TOKEN
from logger import app_logger
from scraper import GithubReposScraper


async def main():
    scraper = GithubReposScraper(access_token=GITHUB_ACCESS_TOKEN)

    try:
        repositories = await scraper.get_repositories()
        for i, repo in enumerate(repositories, start=1):
            print(f"{i:04}. {repo}", flush=True)

    except Exception as e:
        app_logger.error(f"An error occurred: {e}")
        raise

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
