from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
import structlog

logger = structlog.get_logger(__name__)

_CONFIG_CACHE: Dict[str, StoreConfig] = {}

def get_project_root() -> Path:
    """Traverse up to find the project root directory containing docker-compose.yml."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "docker-compose.yml").exists():
            return current
        current = current.parent
    return Path(os.getcwd())

class StoreConfig:
    """Centralized, type-safe Store Configuration object."""

    def __init__(self, raw_config: Dict[str, Any]) -> None:
        self.raw = raw_config
        self.store_id: str = raw_config.get("store_id", "")
        self.store_name: str = raw_config.get("store_name", "")
        self.layout_image: str = raw_config.get("layout_image", "")
        self.cameras: Dict[str, Any] = raw_config.get("cameras", {})
        self.zones: Dict[str, Any] = raw_config.get("zones", {})
        self.analytics: Dict[str, Any] = raw_config.get("analytics", {})
        self.dashboard: Dict[str, Any] = raw_config.get("dashboard", {})

    def get_billing_zone_id(self) -> str:
        """Returns the logical zone ID representing the checkout/billing area."""
        # Check zones for matching names first
        for zone_id in self.zones:
            if "checkout" in zone_id or "billing" in zone_id:
                return zone_id
        # Fallback to scanning camera configs
        for cam_cfg in self.cameras.values():
            if cam_cfg.get("type") == "BILLING":
                return "checkout"
        return "checkout"

    def get_zone_display_name(self, zone_id: str) -> str:
        """Helper to get a user-friendly display name for a zone."""
        if zone_id in self.zones:
            return self.zones[zone_id].get("display_name", zone_id)
        return zone_id.replace("_", " ").title()


def load_store_config(store_id: str) -> StoreConfig:
    """Loads and caches the store_config.yaml for a given store_id."""
    if store_id in _CONFIG_CACHE:
        return _CONFIG_CACHE[store_id]

    root = get_project_root()
    config_path = root / "data" / store_id / "metadata" / "store_config.yaml"

    if not config_path.exists():
        # Look in data/store_x/store_config.yaml as fallback
        alt_path = root / "data" / store_id / "store_config.yaml"
        if alt_path.exists():
            config_path = alt_path
        else:
            raise FileNotFoundError(f"Configuration file not found for store: {store_id} at {config_path}")

    logger.info("loading_store_config", store_id=store_id, path=str(config_path))
    with open(config_path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    config = StoreConfig(raw_data)
    _CONFIG_CACHE[store_id] = config
    return config
