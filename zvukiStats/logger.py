import logging

from rich.logging import RichHandler


logging.basicConfig(
    level="NOTSET",
    format="%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[RichHandler(markup=True)]
)

logger = logging.getLogger("rich")
