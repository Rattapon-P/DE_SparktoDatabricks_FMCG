"""Centralized logging configuration shared by every pipeline job."""
import logging
import logging.config
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"


def setup_logging(job_name: str, log_level: str = "INFO") -> logging.Logger:
    """Configure console + file handlers and return a logger named after the job.

    Console gets a compact INFO+ stream. File gets a detailed DEBUG+ stream
    (separate file per run, timestamped). py4j and pyspark Java loggers are
    quieted to WARN so business logs aren't drowned out.
    """
    LOG_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{job_name}_{timestamp}.log"

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {
                "format": "%(asctime)s | %(levelname)-8s | %(message)s",
                "datefmt": "%H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": str(log_file),
                "mode": "w",
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "py4j": {"level": "WARN"},
            "pyspark": {"level": "WARN"},
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console", "file"],
        },
    }

    logging.config.dictConfig(config)
    logger = logging.getLogger(job_name)
    logger.info("Logger initialized -- log file: %s", log_file)
    return logger
