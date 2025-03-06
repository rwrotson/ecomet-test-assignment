"""Just a basic standard library logger, nothing fancy. In a real-world app
the configuration would be much more complex, maybe using external deps like
loguru or structlog"""

import logging


logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger("app_logger")
