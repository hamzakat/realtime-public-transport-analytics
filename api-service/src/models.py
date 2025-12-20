"""Pydantic models for API requests and responses."""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class MetricRecord(BaseModel):
    """A single metric record."""
    
    timestamp: datetime
    route: str
    direction: str
    window_type: str
    vehicle_count: int
    active_vehicles: int
    avg_speed: float
    avg_delay: float
    min_delay: int
    max_delay: int


class MetricsQueryParams(BaseModel):
    """Query parameters for metrics endpoint."""
    
    route: Optional[str] = Field(None, description="Filter by route number")
    direction: Optional[str] = Field(None, description="Filter by direction (1 or 2)")
    window_type: Optional[str] = Field(None, description="Filter by window type (tumbling_10s or sliding_60s)")
    start_time: Optional[datetime] = Field(None, description="Start of time range (ISO format)")
    end_time: Optional[datetime] = Field(None, description="End of time range (ISO format)")
    limit: int = Field(1000, ge=1, le=10000, description="Maximum number of records to return")


class MetricsResponse(BaseModel):
    """Response model for metrics query."""
    
    count: int
    metrics: List[MetricRecord]


class RouteStats(BaseModel):
    """Statistics for a specific route."""
    
    route: str
    total_observations: int
    avg_vehicle_count: float
    avg_speed: float
    avg_delay: float


class HealthResponse(BaseModel):
    """Health check response."""
    
    status: str
    timestamp: datetime
    influxdb_connected: bool
