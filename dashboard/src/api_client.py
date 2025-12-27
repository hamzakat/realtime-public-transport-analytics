"""API client for fetching historical data."""

import logging
from typing import List, Dict, Optional
from datetime import datetime
import requests


class APIClient:
    """Client for communicating with the API service."""

    def __init__(self, base_url: str, api_key: Optional[str] = None):
        """Initialize API client."""
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def get_routes(self) -> List[str]:
        """Fetch list of available routes."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/routes",
                headers=self._get_headers(),
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch routes: {e}")
            return []

    def get_metrics(
        self,
        route: Optional[str] = None,
        direction: Optional[str] = None,
        window_type: Optional[str] = None,
        limit: int = 1000,
        start_time: Optional[datetime] = None,
    ) -> List[Dict]:
        """Fetch metrics with optional filters."""
        try:
            params = {"limit": limit}
            if route:
                params["route"] = route
            if direction:
                params["direction"] = direction
            if window_type:
                params["window_type"] = window_type
            if start_time:
                params["start_time"] = start_time.isoformat()

            response = requests.get(
                f"{self.base_url}/api/v1/metrics",
                headers=self._get_headers(),
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("metrics", [])
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch metrics: {e}")
            return []

    def get_route_stats(self, route: str) -> Optional[Dict]:
        """Fetch statistics for a specific route."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/routes/{route}/stats",
                headers=self._get_headers(),
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch route stats for {route}: {e}")
            return None
