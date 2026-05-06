"""Orchestrate full Bronze -> Silver -> Gold -> Serve pipeline.

Each stage is its own module with its own Spark session and log file. This script
just chains them, exits non-zero on any failure (so an Airflow / cron runner can
mark the task failed), and summarizes total wall-clock time.
"""
import importlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_config import setup_logging

# importlib because module names start with digits, which break `from pipelines import 01_bronze`
STAGES = [
    ("Bronze ingestion", "pipelines.01_bronze"),
    ("Silver transform", "pipelines.02_silver"),
    ("Gold aggregate", "pipelines.03_gold"),
    ("Serve to Postgres", "pipelines.04_serve"),
]


def main() -> None:
    logger = setup_logging("pipeline_run")
    logger.info("PIPELINE START")
    overall_start = time.time()

    try:
        for i, (label, module_name) in enumerate(STAGES, 1):
            logger.info("Stage %d/%d: %s (%s)", i, len(STAGES), label, module_name)
            stage_start = time.time()
            module = importlib.import_module(module_name)
            module.main()
            logger.info("Stage %d/%d done in %.1fs", i, len(STAGES), time.time() - stage_start)

        elapsed = time.time() - overall_start
        logger.info("PIPELINE SUCCESS in %.1fs (%.1f min)", elapsed, elapsed / 60)

    except Exception:
        logger.error("Pipeline failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
