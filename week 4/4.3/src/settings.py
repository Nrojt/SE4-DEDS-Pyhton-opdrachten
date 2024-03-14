from pathlib import Path
from loguru import logger

class settings():
  basedir = Path.cwd()
  rawdir = Path("raw_data")
  processeddir = Path("processed_data")
  logdir = basedir / "logs"


settings = settings()
logger.add("logfile.log")