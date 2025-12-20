"""FastAPI application for querying historical transport metrics."""
import logging
import sys
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError

from .config import Config
from .models import (
    MetricsResponse, MetricRecord, RouteStats,
    HealthResponse, MetricsQueryParams
)


# Initialize configuration and logging
config = Config.from_env()

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="HSL Transport Analytics API",
    description="REST API for querying historical public transport metrics",
    version="1.0.0"
)

# Global InfluxDB client
influxdb_client: Optional[InfluxDBClient] = None


def get_influxdb_client() -> InfluxDBClient:
    """Get or create InfluxDB client."""
    global influxdb_client
    
    if influxdb_client is None:
        try:
            influxdb_client = InfluxDBClient(
                url=config.influxdb_url,
                token=config.influxdb_token,
                org=config.influxdb_org
            )
            logger.info(f"Connected to InfluxDB at {config.influxdb_url}")
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            raise
    
    return influxdb_client


@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup."""
    logger.info("Starting API service...")
    try:
        get_influxdb_client()
        logger.info("API service started successfully")
    except Exception as e:
        logger.error(f"Failed to start API service: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    global influxdb_client
    
    logger.info("Shutting down API service...")
    if influxdb_client:
        influxdb_client.close()
    logger.info("API service stopped")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    influxdb_connected = False
    
    try:
        client = get_influxdb_client()
        # Try a simple query to verify connection
        query_api = client.query_api()
        query = f'from(bucket: "{config.influxdb_bucket}") |> range(start: -1m) |> limit(n: 1)'
        list(query_api.query(query))
        influxdb_connected = True
    except Exception as e:
        logger.warning(f"Health check failed: {e}")
    
    return HealthResponse(
        status="healthy" if influxdb_connected else "degraded",
        timestamp=datetime.now(timezone.utc),
        influxdb_connected=influxdb_connected
    )


@app.get("/api/v1/metrics", response_model=MetricsResponse)
async def get_metrics(
    route: Optional[str] = Query(None, description="Filter by route number"),
    direction: Optional[str] = Query(None, description="Filter by direction (1 or 2)"),
    window_type: Optional[str] = Query(None, description="Filter by window type"),
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum records to return")
):
    """
    Query vehicle metrics with optional filters.
    
    Returns aggregated vehicle position metrics including vehicle counts,
    average speeds, and delays for specified routes and time ranges.
    """
    try:
        client = get_influxdb_client()
        query_api = client.query_api()
        
        # Build Flux query
        time_range = "range(start: -24h)"
        if start_time and end_time:
            start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            time_range = f'range(start: {start_str}, stop: {end_str})'
        elif start_time:
            start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            time_range = f'range(start: {start_str})'
        elif end_time:
            end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            time_range = f'range(start: -24h, stop: {end_str})'
        
        # Build filter conditions
        filters = []
        if route:
            filters.append(f'r.route == "{route}"')
        if direction:
            filters.append(f'r.direction == "{direction}"')
        if window_type:
            filters.append(f'r.window_type == "{window_type}"')
        
        filter_str = ""
        if filters:
            filter_str = "|> filter(fn: (r) => " + " and ".join(filters) + ")"
        
        query = f'''
            from(bucket: "{config.influxdb_bucket}")
              |> {time_range}
              |> filter(fn: (r) => r._measurement == "vehicle_metrics")
              {filter_str}
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> limit(n: {limit})
        '''
        
        logger.debug(f"Executing query: {query}")
        
        # Execute query
        tables = query_api.query(query)
        
        # Parse results
        metrics = []
        for table in tables:
            for record in table.records:
                try:
                    metric = MetricRecord(
                        timestamp=record.get_time(),
                        route=record.values.get("route", ""),
                        direction=record.values.get("direction", ""),
                        window_type=record.values.get("window_type", ""),
                        vehicle_count=int(record.values.get("vehicle_count", 0)),
                        active_vehicles=int(record.values.get("active_vehicles", 0)),
                        avg_speed=float(record.values.get("avg_speed", 0.0)),
                        avg_delay=float(record.values.get("avg_delay", 0.0)),
                        min_delay=int(record.values.get("min_delay", 0)),
                        max_delay=int(record.values.get("max_delay", 0))
                    )
                    metrics.append(metric)
                except Exception as e:
                    logger.warning(f"Failed to parse record: {e}")
                    continue
        
        return MetricsResponse(
            count=len(metrics),
            metrics=metrics
        )
        
    except InfluxDBError as e:
        logger.error(f"InfluxDB query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/v1/routes", response_model=List[str])
async def get_routes():
    """
    Get list of all routes with available data.
    """
    try:
        client = get_influxdb_client()
        query_api = client.query_api()
        
        query = f'''
            from(bucket: "{config.influxdb_bucket}")
              |> range(start: -24h)
              |> filter(fn: (r) => r._measurement == "vehicle_metrics")
              |> keep(columns: ["route"])
              |> distinct(column: "route")
        '''
        
        tables = query_api.query(query)
        
        routes = set()
        for table in tables:
            for record in table.records:
                route = record.values.get("route")
                if route:
                    routes.add(route)
        
        return sorted(list(routes))
        
    except InfluxDBError as e:
        logger.error(f"InfluxDB query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/v1/routes/{route}/stats", response_model=RouteStats)
async def get_route_stats(
    route: str,
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range")
):
    """
    Get aggregated statistics for a specific route.
    """
    try:
        client = get_influxdb_client()
        query_api = client.query_api()
        
        # Build time range
        time_range = "range(start: -24h)"
        if start_time and end_time:
            start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            time_range = f'range(start: {start_str}, stop: {end_str})'
        
        query = f'''
            from(bucket: "{config.influxdb_bucket}")
              |> {time_range}
              |> filter(fn: (r) => r._measurement == "vehicle_metrics")
              |> filter(fn: (r) => r.route == "{route}")
              |> filter(fn: (r) => r._field == "vehicle_count" or r._field == "avg_speed" or r._field == "avg_delay")
              |> mean()
        '''
        
        tables = query_api.query(query)
        
        stats = {
            "total_observations": 0,
            "avg_vehicle_count": 0.0,
            "avg_speed": 0.0,
            "avg_delay": 0.0
        }
        
        for table in tables:
            for record in table.records:
                field = record.get_field()
                value = record.get_value()
                
                if field == "vehicle_count":
                    stats["avg_vehicle_count"] = value
                    stats["total_observations"] = int(value)
                elif field == "avg_speed":
                    stats["avg_speed"] = value
                elif field == "avg_delay":
                    stats["avg_delay"] = value
        
        return RouteStats(
            route=route,
            total_observations=stats["total_observations"],
            avg_vehicle_count=stats["avg_vehicle_count"],
            avg_speed=stats["avg_speed"],
            avg_delay=stats["avg_delay"]
        )
        
    except InfluxDBError as e:
        logger.error(f"InfluxDB query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "HSL Transport Analytics API",
        "version": "1.0.0",
        "documentation": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)