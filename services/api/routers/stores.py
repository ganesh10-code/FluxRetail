"""
Stores router — serves store configuration and dynamic schemas.
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, HTTPException

from config_loader import load_store_config

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/stores", tags=["Stores"])


@router.get(
    "/{store_id}/config",
    summary="Get store topology configuration",
    description="Returns the raw YAML configuration loaded for the specified store.",
)
async def get_store_config(store_id: str) -> dict:
    """Retrieve configuration details (cameras, zones, layout) for a store."""
    try:
        cfg = load_store_config(store_id)
        return cfg.raw
    except FileNotFoundError as exc:
        logger.warning("store_config_not_found", store_id=store_id, error=str(exc))
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("store_config_load_failed", store_id=store_id, error=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to load configuration: {str(exc)}")
