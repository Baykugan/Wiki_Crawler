import logging
import datetime
from pathlib import Path

PATH = Path(__file__).parent.resolve()
LAUNCHTIME = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
log_directory = PATH / "logs"
log_directory.mkdir(parents=True, exist_ok=True)

log_filename = log_directory / f"{LAUNCHTIME}.log"

log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

file_handler = logging.FileHandler(log_filename, mode="a")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_formatter)

logger = logging.getLogger("root")
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

logger.info("Logging configuration setup complete.")
