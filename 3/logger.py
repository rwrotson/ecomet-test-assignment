"""Just a basic standard library logger, nothing fancy. In a real-world app
the configuration would be much more complex, maybe using external deps like
loguru or structlog"""

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

app_logger = logging.getLogger("app_logger")
db_logger = logging.getLogger("db_logger")
scraper_logger = logging.getLogger("scraper_logger")
utils_logger = logging.getLogger("utils_logger")

worker_logger = logging.getLogger("worker_logger")
processor_logger = logging.getLogger("processor_logger")
