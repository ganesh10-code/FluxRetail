"""
FluxRetail Pipeline Service — entrypoint.

This file is intentionally minimal. All logic lives in orchestrator.py.

Startup sequence:
  1. Load settings (from env / .env file via pydantic-settings)
  2. Configure structlog
  3. Create and run the PipelineOrchestrator

Environment variables (see config.py for full list):
  PIPELINE_MODE      live | replay   (default: replay)
  VIDEO_PATH         path to CCTV video file (LIVE mode)
  EVENTS_JSONL_PATH  path to events file (REPLAY mode)
  KAFKA_BOOTSTRAP_SERVERS  kafka:9092
  LOG_LEVEL          DEBUG | INFO | WARNING | ERROR
  LOG_FORMAT         json | console
"""

from config import settings
from logging_config import configure_logging
from orchestrator import PipelineOrchestrator


def main() -> None:
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    orchestrator = PipelineOrchestrator(settings)
    orchestrator.run()


if __name__ == "__main__":
    main()
