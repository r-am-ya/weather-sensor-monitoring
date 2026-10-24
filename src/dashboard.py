"""Streamlit dashboard for simulated real-time weather monitoring."""

import time
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

from src.database import get_connection

st.set_page_config(
    page_title="Weather Stream Monitor",
    page_icon="🌦️",
    layout="wide",
)

REFRESH_SECONDS = 5
STREAM_STALE_SECONDS = 30
CHART_GAP_SECONDS = 5
CHART_WINDOW_MINUTES = 5


@st.cache_data(ttl=5)
def load_data():
    """Fetch recent readings and current station metrics from MySQL."""
    connection = get_connection()

    try:
        readings = pd.read_sql(
            """
            SELECT event_id, station_id, station_name, event_timestamp,
                   temperature_c, humidity_pct, wind_speed_kmh, heat_index_c,
                   rolling_avg_temp_c, rolling_avg_humidity_pct,
                   rolling_avg_wind_kmh, peak_wind_kmh,
                   is_anomaly, anomaly_score, anomaly_scenario
            FROM weather_readings
            WHERE event_timestamp >= UTC_TIMESTAMP() - INTERVAL 5 MINUTE
            ORDER BY event_timestamp DESC
            """,
            connection,
        )

        stations = pd.read_sql(
            """
            SELECT station_id, station_name, latest_timestamp,
                   temperature_c, humidity_pct, wind_speed_kmh, heat_index_c,
                   rolling_avg_temp_c, rolling_avg_humidity_pct,
                   rolling_avg_wind_kmh, peak_temp_c, peak_wind_kmh,
                   total_readings, anomaly_count, last_is_anomaly, updated_at
            FROM station_metrics
            ORDER BY station_name
            """,
            connection,
        )

        return readings, stations

    finally:
        connection.close()


# Load data
try:
    readings, stations = load_data()

except Exception as exc:
    st.error("Unable to load dashboard data from MySQL.")
    st.exception(exc)
    st.stop()


if stations.empty:
    st.warning(
        "No processed readings are available yet. "
        "Start the Kafka producer and stream processor."
    )
    st.stop()


# ---------------------------------------------------------
# Stream health
# ---------------------------------------------------------
latest_timestamp = pd.to_datetime(
    stations["latest_timestamp"],
    utc=True,
    errors="coerce",
).max()

now = datetime.now(timezone.utc)

if pd.isna(latest_timestamp):
    age_seconds = float("inf")
else:
    age_seconds = max(
        0,
        (now - latest_timestamp.to_pydatetime()).total_seconds(),
    )

if age_seconds <= STREAM_STALE_SECONDS:
    st.success(
        f"Stream running · Latest reading {age_seconds:.0f}s ago"
    )
else:
    st.error(
        "Stream stopped or delayed · Latest reading was "
        f"{age_seconds:.0f}s ago"
    )


# ---------------------------------------------------------
# Dashboard title and KPI cards
# ---------------------------------------------------------
st.title("🌦️ Real-Time Weather Sensor Monitoring")

st.caption(
    "Streaming analytics and Isolation Forest anomaly monitoring "
    "· All readings are simulated"
)

total_readings = int(stations["total_readings"].fillna(0).sum())
total_anomalies = int(stations["anomaly_count"].fillna(0).sum())

col1, col2, col3, col4 = st.columns(4)

col1.metric("Stations reporting", len(stations))
col2.metric("Readings recorded", f"{total_readings:,}")
col3.metric("Anomalies flagged", f"{total_anomalies:,}")

col4.metric(
    "Latest temperature",
    f"{stations['temperature_c'].mean():.1f} °C",
    help="Mean of the latest temperature recorded at each station",
)


# ---------------------------------------------------------
# Current station conditions
# ---------------------------------------------------------
st.subheader("Current station conditions")

display_columns = [
    "station_id",
    "station_name",
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
    "heat_index_c",
    "rolling_avg_temp_c",
    "peak_wind_kmh",
    "total_readings",
    "anomaly_count",
]

st.dataframe(
    stations[display_columns],
    use_container_width=True,
    hide_index=True,
)


# ---------------------------------------------------------
# Weather trends
# ---------------------------------------------------------
st.subheader("Weather trends")

if readings.empty:
    st.info("No readings have been saved in the last five minutes.")

else:
    readings["event_timestamp"] = pd.to_datetime(
        readings["event_timestamp"],
        utc=True,
        errors="coerce",
    )

    readings = readings.dropna(
        subset=["event_timestamp", "station_id"]
    )

    selected_station = st.selectbox(
        "Choose a station",
        options=stations["station_id"].tolist(),
        format_func=lambda sid: (
            f"{sid} — "
            + stations.loc[
                stations["station_id"] == sid,
                "station_name",
            ].iloc[0]
        ),
    )

    station_data = readings[
        readings["station_id"] == selected_station
    ].copy()

    if station_data.empty:
        st.info(
            "No readings for this station in the last five minutes. "
            "Choose another station or wait for new readings."
        )

    else:
        station_data = station_data.sort_values(
            "event_timestamp"
        ).copy()

        latest_time = station_data["event_timestamp"].max()

        cutoff_time = latest_time - pd.Timedelta(
            minutes=CHART_WINDOW_MINUTES
        )

        station_data = station_data[
            station_data["event_timestamp"] >= cutoff_time
        ].copy()

        # Convert chart values to numeric safely.
        for column in [
            "temperature_c",
            "humidity_pct",
            "wind_speed_kmh",
            "rolling_avg_temp_c",
        ]:
            station_data[column] = pd.to_numeric(
                station_data[column],
                errors="coerce",
            )

        # Break chart lines across genuine time gaps.
        gap_mask = (
            station_data["event_timestamp"].diff()
            > pd.Timedelta(seconds=CHART_GAP_SECONDS)
        )

        for column in [
            "temperature_c",
            "humidity_pct",
            "wind_speed_kmh",
            "rolling_avg_temp_c",
        ]:
            station_data.loc[gap_mask, column] = float("nan")

        # Exclude physically invalid humidity values from the chart.
        station_data["humidity_plot"] = (
            station_data["humidity_pct"].where(
                station_data["humidity_pct"].between(0, 100)
            )
        )

        station_data["anomaly_flag"] = (
            station_data["is_anomaly"]
            .fillna(False)
            .astype(bool)
        )

        station_data["anomaly_temperature"] = (
            station_data["temperature_c"].where(
                station_data["anomaly_flag"]
            )
        )

        chart1, chart2 = st.columns(2)

        # Temperature chart with anomaly markers.
        with chart1:
            fig_temp = px.line(
                station_data,
                x="event_timestamp",
                y="temperature_c",
                title="Temperature over time",
                labels={
                    "event_timestamp": "Time (UTC)",
                    "temperature_c": "Temperature (°C)",
                },
            )

            fig_temp.update_traces(
                connectgaps=False,
                line={"width": 2},
                name="Temperature",
            )

            # Add rolling average if available.
            if station_data["rolling_avg_temp_c"].notna().any():
                fig_temp.add_scatter(
                    x=station_data["event_timestamp"],
                    y=station_data["rolling_avg_temp_c"],
                    mode="lines",
                    name="Rolling average",
                    line={"width": 2, "dash": "dash"},
                    connectgaps=False,
                )

            fig_temp.add_scatter(
                x=station_data["event_timestamp"],
                y=station_data["anomaly_temperature"],
                mode="markers",
                name="Flagged anomaly",
                marker={
                    "color": "red",
                    "size": 9,
                    "symbol": "circle",
                },
                connectgaps=False,
            )

            fig_temp.update_layout(
                hovermode="x unified",
                margin={"l": 10, "r": 10, "t": 55, "b": 10},
                legend_title_text="Series",
            )

            st.plotly_chart(
                fig_temp,
                use_container_width=True,
            )

        # Humidity chart.
        with chart2:
            fig_humidity = px.line(
                station_data,
                x="event_timestamp",
                y="humidity_plot",
                title="Humidity over time",
                labels={
                    "event_timestamp": "Time (UTC)",
                    "humidity_plot": "Humidity (%)",
                },
            )

            fig_humidity.update_traces(
                connectgaps=False,
                line={"width": 2},
            )

            fig_humidity.update_yaxes(
                range=[0, 100],
                ticksuffix="%",
            )

            fig_humidity.update_layout(
                hovermode="x unified",
                margin={"l": 10, "r": 10, "t": 55, "b": 10},
            )

            st.plotly_chart(
                fig_humidity,
                use_container_width=True,
            )

        # Wind speed chart.
        fig_wind = px.line(
            station_data,
            x="event_timestamp",
            y="wind_speed_kmh",
            title="Wind speed over time",
            labels={
                "event_timestamp": "Time (UTC)",
                "wind_speed_kmh": "Wind speed (km/h)",
            },
        )

        fig_wind.update_traces(
            connectgaps=False,
            line={"width": 2},
        )

        fig_wind.update_layout(
            hovermode="x unified",
            margin={"l": 10, "r": 10, "t": 55, "b": 10},
        )

        st.plotly_chart(
            fig_wind,
            use_container_width=True,
        )


# ---------------------------------------------------------
# Anomaly alerts
# ---------------------------------------------------------
st.subheader("Anomaly alerts")

if readings.empty:
    st.info("No readings are available in the last five minutes.")

else:
    anomalies = readings[
        readings["is_anomaly"].fillna(False).astype(bool)
        | readings["anomaly_scenario"].notna()
    ].copy()

    if anomalies.empty:
        st.info("No flagged anomalies in the last five minutes.")

    else:
        anomaly_columns = [
            "event_timestamp",
            "station_id",
            "station_name",
            "temperature_c",
            "humidity_pct",
            "wind_speed_kmh",
            "is_anomaly",
            "anomaly_score",
            "anomaly_scenario",
        ]

        st.dataframe(
            anomalies[anomaly_columns].sort_values(
                "event_timestamp",
                ascending=False,
            ),
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------
st.caption(
    "Data is simulated for academic demonstration. "
    "Anomaly flags are model outputs, not official weather warnings."
)


# ---------------------------------------------------------
# Automatic refresh
# ---------------------------------------------------------
time.sleep(REFRESH_SECONDS)
st.rerun()