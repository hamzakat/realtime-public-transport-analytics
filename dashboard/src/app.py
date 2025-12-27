"""Streamlit dashboard for HSL transport analytics."""

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import Config
from kafka_consumer import KafkaDataConsumer
from api_client import APIClient


def setup_logging(log_level: str):
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )


@st.cache_resource
def initialize_connections():
    """Initialize API client and Kafka consumer (cached)."""
    config = Config.from_env()

    setup_logging(config.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Initializing dashboard connections...")

    api_client = APIClient(config.api_url, config.api_key)
    kafka_consumer = KafkaDataConsumer(
        config.kafka_bootstrap_servers, config.kafka_topic
    )

    connected = kafka_consumer.connect()
    if connected:
        logger.info("Kafka consumer connected successfully")
    else:
        logger.warning("Kafka consumer connection failed, using API only")

    logger.info("Dashboard connections initialized")
    return api_client, kafka_consumer


@st.cache_data(ttl=300)
def get_routes_cached(_api_client: APIClient) -> List[str]:
    """Fetch available routes with caching."""
    routes = _api_client.get_routes()
    return sorted(routes) if routes else []


def filter_live_data(
    messages: List[Dict], selected_route: str, selected_direction: str
) -> List[Dict]:
    """Filter live Kafka messages by route and direction."""
    filtered = []
    for msg in messages:
        try:
            route = str(msg.get("route", ""))
            direction = str(msg.get("direction", ""))

            if (not selected_route or route == selected_route) and (
                not selected_direction or direction == selected_direction
            ):
                filtered.append(msg)
        except Exception as e:
            st.warning(f"Error filtering message: {e}")
    return filtered


def create_metrics_dataframe(metrics: List[Dict]) -> pd.DataFrame:
    """Convert metrics list to pandas DataFrame."""
    if not metrics:
        return pd.DataFrame()

    df = pd.DataFrame(metrics)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    else:
        df["timestamp"] = datetime.now(timezone.utc)

    return df


def display_statistics(df: pd.DataFrame):
    """Display basic statistics."""
    if df.empty:
        st.info("No data available")
        return

    cols = st.columns(4)

    with cols[0]:
        st.metric("Total Records", len(df), help="Total number of metric records")

    if "vehicle_count" in df.columns:
        with cols[1]:
            avg_vehicles = df["vehicle_count"].mean()
            st.metric(
                "Avg Vehicle Count",
                f"{avg_vehicles:.1f}",
                help="Average number of vehicles across selected data",
            )

    if "avg_speed" in df.columns:
        with cols[2]:
            avg_speed = df["avg_speed"].mean()
            st.metric("Avg Speed", f"{avg_speed:.2f} m/s", help="Average vehicle speed")

    if "avg_delay" in df.columns:
        with cols[3]:
            avg_delay = df["avg_delay"].mean()
            delay_color = "normal" if avg_delay >= 0 else "inverse"
            st.metric(
                "Avg Delay",
                f"{avg_delay:.1f} s",
                delta_color=delay_color,
                help="Average delay from schedule (negative = early)",
            )


def display_charts(df: pd.DataFrame, selected_route: str, selected_direction: str):
    """Display charts for the selected data."""
    if df.empty:
        st.info("No data available for charts")
        return

    tab1, tab2, tab3 = st.tabs(["Vehicle Count", "Speed", "Delay"])

    with tab1:
        if "vehicle_count" in df.columns and "timestamp" in df.columns:
            chart_df = df.sort_values("timestamp")
            fig = px.line(
                chart_df,
                x="timestamp",
                y="vehicle_count",
                color="window_type" if "window_type" in df.columns else None,
                title=f"Vehicle Count Over Time - Route {selected_route}",
                markers=True,
            )
            fig.update_layout(xaxis_title="Time", yaxis_title="Vehicle Count")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Vehicle count data not available")

    with tab2:
        if "avg_speed" in df.columns and "timestamp" in df.columns:
            chart_df = df.sort_values("timestamp")
            fig = px.line(
                chart_df,
                x="timestamp",
                y="avg_speed",
                color="window_type" if "window_type" in df.columns else None,
                title=f"Average Speed Over Time - Route {selected_route}",
                markers=True,
            )
            fig.update_layout(xaxis_title="Time", yaxis_title="Average Speed (m/s)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Speed data not available")

    with tab3:
        if "avg_delay" in df.columns and "timestamp" in df.columns:
            chart_df = df.sort_values("timestamp")
            fig = px.line(
                chart_df,
                x="timestamp",
                y="avg_delay",
                color="window_type" if "window_type" in df.columns else None,
                title=f"Average Delay Over Time - Route {selected_route}",
                markers=True,
            )
            fig.add_hline(
                y=0, line_dash="dash", line_color="red", annotation_text="On Time"
            )
            fig.update_layout(xaxis_title="Time", yaxis_title="Average Delay (seconds)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Delay data not available")


def display_raw_data(df: pd.DataFrame):
    """Display raw data table."""
    if df.empty:
        return

    st.subheader("Raw Data")

    display_df = df.copy()

    if "timestamp" in display_df.columns:
        display_df["timestamp"] = display_df["timestamp"].dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    st.dataframe(display_df, use_container_width=True, height=400)


def main():
    """Main dashboard application."""
    st.set_page_config(
        page_title="HSL Transport Analytics Dashboard",
        page_icon="🚌",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🚌 HSL Public Transport Analytics")
    st.markdown(
        "Real-time and historical analytics for [Helsinki Region Transport](https://www.hsl.fi/en/hsl/open-data)"
    )

    api_client, kafka_consumer = initialize_connections()

    st.sidebar.header("Filters")

    routes = get_routes_cached(api_client)

    if not routes:
        st.error(
            "No routes available. Please ensure the API service is running and has data."
        )
        st.stop()

    st.sidebar.header("Data Source")

    data_source = st.sidebar.radio(
        "Select Data Source",
        options=["Live (Kafka)", "Historical (API)"],
        index=0,
        help="Choose between real-time streaming data or historical queries",
    )

    if data_source == "Live (Kafka)":
        selected_route = st.sidebar.selectbox(
            "Route Number",
            options=["All Routes"] + routes,
            index=0,
            help="Select a specific route to filter data",
        )

        direction_options = ["All Directions", "1", "2"]
        selected_direction = st.sidebar.selectbox(
            "Direction",
            options=direction_options,
            index=0,
            help="Select direction (1 or 2) to filter data",
        )
    else:
        selected_route = st.sidebar.selectbox(
            "Route Number",
            options=routes,
            index=0,
            help="Select a specific route to filter data",
        )

        direction_options = ["1", "2"]
        selected_direction = st.sidebar.selectbox(
            "Direction",
            options=direction_options,
            index=0,
            help="Select direction (1 or 2) to filter data",
        )

    st.sidebar.header("Auto Refresh")

    if data_source == "Live (Kafka)":
        auto_refresh = st.sidebar.checkbox(
            "Enable Auto Refresh",
            value=True,
            help="Automatically refresh data every 5 seconds",
        )

        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=2,
            max_value=30,
            value=5,
            help="Set how often to refresh live data",
        )
    else:
        auto_refresh = False

        st.sidebar.subheader("Time Range")
        time_range_unit = st.sidebar.radio(
            "Time Range Unit",
            options=["Hours", "Days"],
            index=0,
            help="Select unit for time range",
        )

        if time_range_unit == "Hours":
            time_range_value = st.sidebar.slider(
                "Time Range",
                min_value=1,
                max_value=24,
                value=1,
                help="How many hours of historical data to fetch",
            )
        else:
            time_range_value = st.sidebar.slider(
                "Time Range",
                min_value=1,
                max_value=7,
                value=1,
                help="How many days of historical data to fetch",
            )

    route_filter = selected_route if selected_route != "All Routes" else None
    direction_filter = (
        selected_direction if selected_direction != "All Directions" else None
    )

    start_time = None
    if data_source == "Historical (API)":
        from datetime import datetime, timezone, timedelta

        if time_range_unit == "Hours":
            start_time = datetime.now(timezone.utc) - timedelta(hours=time_range_value)
        else:
            start_time = datetime.now(timezone.utc) - timedelta(days=time_range_value)

    if data_source == "Live (Kafka)":
        st.header("📡 Live Data")
        st.info("Consuming real-time data from Kafka topic")

        messages = kafka_consumer.consume_messages(timeout=1.0)

        if messages:
            filtered_messages = filter_live_data(
                messages, route_filter or "", direction_filter or ""
            )
            df = create_metrics_dataframe(filtered_messages)

            if not df.empty:
                display_statistics(df)
                st.divider()
                display_raw_data(df)

                if len(messages) > len(filtered_messages):
                    st.caption(
                        f"Showing {len(filtered_messages)} of {len(messages)} messages (filtered by selection)"
                    )

                session_key = f"latest_data_{route_filter}_{direction_filter}"
                st.session_state[session_key] = filtered_messages

            else:
                session_key = f"latest_data_{route_filter}_{direction_filter}"
                if session_key in st.session_state and st.session_state[session_key]:
                    latest_data = st.session_state[session_key]
                    latest_df = create_metrics_dataframe(latest_data)
                    st.info("No new data, showing latest cached values")
                    display_statistics(latest_df)
                    st.divider()
                    display_raw_data(latest_df)
                else:
                    st.warning("No matching data found for selected filters")
        else:
            st.info("Waiting for live data from Kafka...")

        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()

    else:
        st.header("📊 Historical Data")
        time_range_text = f"Past {time_range_value} {time_range_unit.lower()}"
        st.info(f"Querying historical data for: {time_range_text}")

        with st.spinner("Fetching data..."):
            metrics = api_client.get_metrics(
                route=route_filter,
                direction=direction_filter,
                limit=1000,
                start_time=start_time,
            )

        if metrics:
            df = create_metrics_dataframe(metrics)

            if not df.empty:
                display_statistics(df)
                st.divider()
                display_charts(df, selected_route, selected_direction)
                st.divider()
                display_raw_data(df)

                st.caption(f"Showing {len(metrics)} records from API")
            else:
                st.warning("No matching data found for selected filters")
        else:
            st.warning(
                "No data available from API. Ensure the pipeline is running and data exists."
            )


if __name__ == "__main__":
    main()
