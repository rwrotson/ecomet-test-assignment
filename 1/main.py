from contextlib import asynccontextmanager
from os import environ
from typing import Annotated, AsyncGenerator
from urllib.parse import quote_plus

import uvicorn
from asyncpg import create_pool, Connection, Pool
from fastapi import APIRouter, FastAPI, Depends, Request

from logger import logger


def get_pg_dsn() -> str:
    # In a real production environment, I would probably use pydantic-settings to load environment variables.
    # It could also store other params, like min_size, max_size and other params of pg_pool -- but I don't
    # know if we gonna need this in this project, so I'll simply stick with create_pool's defaults.
    # However, pydantic-settings is an additional dependency,
    # and I’m not sure if I can install anything beyond what’s in requirements.txt by default.
    # Additionally, at this moment, I don’t know the deployment details or how environment variables are managed.
    # So for now, I’m assuming that these variables are available in the environment one way or another,
    # e.g., via: export PG_USER=myuser.

    try:
        user = quote_plus(environ["PG_USER"])
        password = quote_plus(environ["PG_PASSWORD"])
        host = quote_plus(environ["PG_HOST"])
        port = quote_plus(environ["PG_PORT"])
        database = quote_plus(environ["PG_DATABASE"])
    except KeyError as e:
        error_message = f"Missing environment variable: {e.args[0]}"
        logger.error(error_message)
        raise RuntimeError(error_message) from e

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


async def init_pool(**kwargs) -> Pool:
    try:
        return await create_pool(dsn=get_pg_dsn(), **kwargs)
    except Exception as e:
        logger.error(f"Failed to initialize the connection pool: {e}")
        raise


async def close_pool(pool: Pool) -> None:
    try:
        await pool.close()
    except Exception as e:
        logger.error(f"Failed to close the connection pool: {e}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    # app.state is not deprecated, nor is it protected or private, so using it meets the assignment requirements.
    # However, it exists mainly for backward compatibility with Starlette,
    # so using middleware to inject pg_pool into requests might be more in line with FastAPI style.
    # That said, I find my current implementation simpler and more explicit,
    # so I’ll stick with this approach unless there are any objections :)

    app.state.pg_pool = await create_pool(dsn=get_pg_dsn())
    yield
    await close_pool(app.state.pg_pool)


async def get_pg_connection(request: Request) -> AsyncGenerator[Connection, None]:
    pool: Pool = request.app.state.pg_pool
    async with pool.acquire() as conn:
        yield conn


async def get_db_version(conn: Annotated[Connection, Depends(get_pg_connection)]) -> str:
    return await conn.fetchval("SELECT version()")


def register_routes(app: FastAPI) -> None:
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version, methods=["GET"])
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
