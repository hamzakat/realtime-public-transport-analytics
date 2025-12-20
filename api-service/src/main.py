"""FastAPI application with authentication, rate limiting, and audit logging."""
import logging
import sys
import json
from datetime import datetime, timezone
from typing import List, Optional
from functools import wraps

from fastapi import FastAPI, HTTPException, Query, Header, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from .config import Config
from .models import (
    MetricsResponse, MetricRecord, RouteStats,
    HealthResponse, MetricsQueryParams
)


# Initialize configuration
config = Config.from_env()

# Initialize structured logging
class AuditLogger:
    """Structured audit logger for API requests."""
    
    def __init__(self):
        self.logger = logging.getLogger("api.audit")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_request(self, request: Request, response_code: int, 
                   user_id: Optional[str] = None, error: Optional[str] = None):
        """Log API request with structured data."""
        audit_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "api_request",
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "client_ip": request.client.host if request.client else "unknown",
            "user_agent": request.headers.get("user-agent", "unknown"),
            "user_id": user_id or "anonymous",
            "response_code": response_code,
            "error": error
        }
        self.logger.info(json.dumps(audit_entry))

audit_logger = AuditLogger()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Initialize FastAPI app
app = FastAPI(
    title="HSL Transport Analytics API",
    description="REST API for querying historical public transport metrics",
    version="1.0.0"
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# API Key authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Global InfluxDB client
influxdb_client: Optional[InfluxDBClient] = None


def verify_api_key(api_key: Optional[str] = Header(None, alias="X-API-Key")) -> str:
    """Verify API key from request header."""
    if not config.require_auth:
        return "public"
    
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header."
        )
    
    if api_key != config.api_key:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )
    
    return api_key


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
            logging.info(f"Connected to InfluxDB at {config.influxdb_url}")
        except Exception as e:
            logging.error(f"Failed to connect to InfluxDB: {e}")
            raise
    
    return influxdb_client


@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    """Log all API requests for audit trail."""
    response = None
    error_msg = None
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        error_msg = str(e)
        response = JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
        return response
    finally:
        if response:
            # Extract user from API key if present
            api_key = request.headers.get("X-API-Key", "anonymous")
            user_id = "authenticated" if api_key != "anonymous" else "anonymous"
            
            audit_logger.log_request(
                request=request,
                response_code=response.status_code if response else 500,
                user_id=user_id,
                error=error_msg
            )


@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup."""
    logging.info(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "service_startup",
        "service": "api-service",
        "version": "1.0.0"
    }))
    
    try:
        get_influxdb_client()
        logging.info("API service started successfully")
    except Exception as e:
        logging.error(f"Failed to start API service: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    global influxdb_client
    
    logging.info(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "service_shutdown",
        "service": "api-service"
    }))
    
    if influxdb_client:
        influxdb_client.close()


@app.get("/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Health check endpoint (no authentication required)."""
    influxdb_connected = False
    
    try:
        client = get_influxdb_client()
        query_api = client.query_api()
        query = f'from(bucket: "{config.influxdb_bucket}") |> range(start: -1m) |> limit(n: 1)'
        list(query_api.query(query))
        influxdb_connected = True
    except Exception as e:
        logging.warning(f"Health check failed: {e}")
    
    return HealthResponse(
        status="healthy" if influxdb_connected else "degraded",
        timestamp=datetime.now(timezone.utc),
        influxdb_connected=influxdb_connected
    )


@app.get("/api/v1/metrics", response_model=MetricsResponse)
@limiter.limit("100/minute")
async def get_metrics(
    request: Request,
    route: Optional[str] = Query(None, description="Filter by route number"),
    direction: Optional[str] = Query(None, description="Filter by direction (1 or 2)"),
    window_type: Optional[str] = Query(None, description="Filter by window type"),
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum records to return"),
    api_key: str = Header(None, alias="X-API-Key")
):
    """Query vehicle metrics with optional filters (requires authentication)."""
    verify_api_key(api_key)
    
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
        
        logging.debug(f"Executing query: {query}")
        
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
                    logging.warning(f"Failed to parse record: {e}")
                    continue
        
        logging.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "query_executed",
            "endpoint": "/api/v1/metrics",
            "records_returned": len(metrics),
            "filters": {"route": route, "direction": direction, "window_type": window_type}
        }))
        
        return MetricsResponse(
            count=len(metrics),
            metrics=metrics
        )
        
    except InfluxDBError as e:
        logging.error(f"InfluxDB query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/v1/routes", response_model=List[str])
@limiter.limit("100/minute")
async def get_routes(
    request: Request,
    api_key: str = Header(None, alias="X-API-Key")
):
    """Get list of all routes with available data (requires authentication)."""
    verify_api_key(api_key)
    
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
        
        route_list = sorted(list(routes))
        
        logging.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "query_executed",
            "endpoint": "/api/v1/routes",
            "routes_returned": len(route_list)
        }))
        
        return route_list
        
    except InfluxDBError as e:
        logging.error(f"InfluxDB query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/v1/routes/{route}/stats", response_model=RouteStats)
@limiter.limit("100/minute")
async def get_route_stats(
    request: Request,
    route: str,
    start_time: Optional[datetime] = Query(None, description="Start of time range"),
    end_time: Optional[datetime] = Query(None, description="End of time range"),
    api_key: str = Header(None, alias="X-API-Key")
):
    """Get aggregated statistics for a specific route (requires authentication)."""
    verify_api_key(api_key)
    
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
        
        logging.info(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "query_executed",
            "endpoint": f"/api/v1/routes/{route}/stats",
            "route": route
        }))
        
        return RouteStats(
            route=route,
            total_observations=stats["total_observations"],
            avg_vehicle_count=stats["avg_vehicle_count"],
            avg_speed=stats["avg_speed"],
            avg_delay=stats["avg_delay"]
        )
        
    except InfluxDBError as e:
        logging.error(f"InfluxDB query error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "HSL Transport Analytics API",
        "version": "1.0.0",
        "documentation": "/docs",
        "health": "/health",
        "authentication": "Required for data endpoints. Use X-API-Key header."
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)