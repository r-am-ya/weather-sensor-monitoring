"""WeatherPulse Streamlit dashboard."""
from datetime import datetime, timezone
import time

import pandas as pd
import plotly.express as px
import streamlit as st

from src.config import DASHBOARD_REFRESH_SECONDS, OPEN_METEO_INTERVAL_SECONDS, SENSOR_INTERVAL_SECONDS, WEATHER_SOURCE
from src.database import get_connection
from src.weather_visuals import build_animated_wind_figure, fetch_bengaluru_current

st.set_page_config(page_title="WeatherPulse", page_icon="🌦️", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@500;600;700;800&display=swap');
.stApp { background: radial-gradient(ellipse at 12% 0%, #15394a 0, #0a1924 38%, #07111b 100%); color:#ecf5f8; }
html, body, [class*="css"] { font-family:'DM Sans',sans-serif; }
.block-container { max-width:1450px; padding-top:2rem; }
.wp-header { display:flex; align-items:center; gap:18px; padding:22px 26px; border:1px solid #264354; border-radius:20px; background:linear-gradient(110deg,rgba(18,49,64,.92),rgba(11,28,40,.88)); margin-bottom:20px; }
.wp-icon { width:68px;height:58px;position:relative;flex:none; }
.wp-cloud { position:absolute;left:8px;top:10px;width:43px;height:27px;border-radius:22px;background:#d9f3f4;box-shadow:0 0 28px #68c8ce55; }
.wp-cloud:before,.wp-cloud:after { content:'';position:absolute;background:#d9f3f4;border-radius:50%; }
.wp-cloud:before { width:24px;height:24px;left:8px;top:-11px; }.wp-cloud:after { width:19px;height:19px;right:3px;top:-6px; }
.wp-dash { position:absolute;height:3px;border-radius:4px;background:#43d9c3;animation:gust 2.8s ease-in-out infinite; }
.wp-dash.one {left:18px;top:44px;width:37px}.wp-dash.two {left:4px;top:51px;width:28px;animation-delay:.5s}
@keyframes gust { 0%,100%{transform:translateX(0);opacity:.5}50%{transform:translateX(8px);opacity:1} }
@media(prefers-reduced-motion:reduce){.wp-dash{animation:none}}
.wp-title { font:800 30px Manrope,sans-serif;letter-spacing:-.7px;color:#f2fbfd; }.wp-sub {color:#a8c2cd;margin-top:3px;font-size:14px}
[data-testid="stMetric"] { background:linear-gradient(145deg,#123043,#102534);border:1px solid #25495a;padding:18px 20px;border-radius:17px; }
[data-testid="stMetricLabel"]{color:#a9c4cf!important}[data-testid="stMetricValue"]{color:#f1fbfc!important;font-family:Manrope,sans-serif}
h2,h3 {font-family:Manrope,sans-serif!important;color:#eaf7fa!important} .stCaption{color:#94aeb9!important}
.status-pill {display:inline-block;border-radius:99px;padding:6px 12px;background:#123c39;color:#71e3c9;border:1px solid #286c5e;font-size:12px;font-weight:700}
.status-stale {display:inline-block;border-radius:99px;padding:6px 12px;background:#44331b;color:#ffd27d;border:1px solid #78602f;font-size:12px;font-weight:700}
</style>
<div class="wp-header"><div class="wp-icon"><div class="wp-cloud"></div><div class="wp-dash one"></div><div class="wp-dash two"></div></div><div><div class="wp-title">WeatherPulse</div><div class="wp-sub">Real-time weather stream analytics</div></div></div>
""", unsafe_allow_html=True)

@st.cache_data(ttl=10, show_spinner=False)
def load_data():
    connection = get_connection()
    try:
        readings = pd.read_sql("""SELECT event_id,station_id,station_name,event_timestamp,
            temperature_c,humidity_pct,wind_speed_kmh,heat_index_c,rolling_avg_temp_c,
            is_anomaly,anomaly_score,anomaly_scenario,source_type,observation_timestamp,
            received_timestamp,wind_direction_deg,created_at FROM weather_readings
            WHERE event_timestamp >= UTC_TIMESTAMP() - INTERVAL 60 MINUTE
            ORDER BY event_timestamp DESC LIMIT 1500""", connection)
        stations = pd.read_sql("""SELECT station_id,station_name,latest_timestamp,temperature_c,
            humidity_pct,wind_speed_kmh,heat_index_c,rolling_avg_temp_c,peak_wind_kmh,
            total_readings,anomaly_count,last_is_anomaly,updated_at FROM station_metrics
            ORDER BY station_name""", connection)
        status = pd.read_sql("""SELECT MAX(created_at) AS last_insert,
            MAX(received_timestamp) AS last_processor_event,
            MAX(observation_timestamp) AS last_observation FROM weather_readings""", connection)
        return readings, stations, status
    finally:
        connection.close()

@st.cache_data(ttl=300, show_spinner=False)
def load_live_current():
    return fetch_bengaluru_current()

try:
    readings, stations, status = load_data()
except Exception as exc:
    st.error("MySQL is unavailable, so pipeline readings cannot be loaded.")
    st.caption("Check Docker Desktop, the MySQL health status, and MYSQL_* settings.")
    st.exception(exc)
    st.stop()

source_label = "Synthetic sensor simulator" if WEATHER_SOURCE == "simulated" else "Open-Meteo current conditions"
recent_insert = pd.to_datetime(status.iloc[0]["last_insert"], utc=True, errors="coerce") if not status.empty else pd.NaT
insert_age = (datetime.now(timezone.utc) - recent_insert.to_pydatetime()).total_seconds() if pd.notna(recent_insert) else None
pipeline_ok = insert_age is not None and insert_age < max(90, DASHBOARD_REFRESH_SECONDS * 4)
recent_observation = pd.to_datetime(status.iloc[0]["last_observation"], utc=True, errors="coerce") if not status.empty else pd.NaT
observation_age = (datetime.now(timezone.utc) - recent_observation.to_pydatetime()).total_seconds() if pd.notna(recent_observation) else None
observation_limit = OPEN_METEO_INTERVAL_SECONDS * 2 + 60 if WEATHER_SOURCE == "open_meteo" else max(60, SENSOR_INTERVAL_SECONDS * 4)
observation_current = observation_age is not None and observation_age <= observation_limit
observation_pill = '<span class="status-pill">● OBSERVATIONS CURRENT</span>' if observation_current else '<span class="status-stale">● OBSERVATIONS DELAYED / WAITING</span>'
st.markdown(f'<span class="status-pill">● {"PIPELINE RECEIVING" if pipeline_ok else "PIPELINE STALE / WAITING"}</span> &nbsp; <span class="status-pill">SOURCE · {source_label}</span> &nbsp; {observation_pill}', unsafe_allow_html=True)
st.caption(f"Dashboard refreshes every {DASHBOARD_REFRESH_SECONDS}s · Saved-data freshness is measured from successful database inserts.")

station_options = stations["station_id"].dropna().tolist() if not stations.empty else ["BLR"]
default_station = station_options.index("BLR") if "BLR" in station_options else 0
control_a, control_b = st.columns([1, 2])
with control_a:
    selected_station = st.selectbox("Location", station_options, index=default_station,
        format_func=lambda value: (stations.loc[stations.station_id == value, "station_name"].iloc[0]
                                   if not stations.empty and (stations.station_id == value).any() else "Bengaluru"))
with control_b:
    st.caption(f"Producer source is configured as **{source_label}** (`WEATHER_SOURCE={WEATHER_SOURCE}`). Change `.env` and restart the producer to switch sources.")
selected_rows = stations[stations["station_id"] == selected_station] if not stations.empty else stations
latest = selected_rows.iloc[0] if not selected_rows.empty else None
anom_recent = int(readings["is_anomaly"].fillna(False).astype(bool).sum()) if not readings.empty else 0
k = st.columns(4)
k[0].metric("Temperature", f"{latest['temperature_c']:.1f} °C" if latest is not None else "—", "Latest station metric")
k[1].metric("Humidity", f"{latest['humidity_pct']:.0f}%" if latest is not None else "—")
k[2].metric("Wind speed", f"{latest['wind_speed_kmh']:.1f} km/h" if latest is not None else "—")
k[3].metric("Pipeline / anomalies", "LIVE" if pipeline_ok else "STALE", f"{anom_recent} flagged in last hour")

left, right = st.columns([1.2, .8])
with left:
    station_name = (stations.loc[stations.station_id == selected_station, "station_name"].iloc[0]
                    if not stations.empty and (stations.station_id == selected_station).any() else "Bengaluru")
    st.subheader(f"{station_name} wind")
    if WEATHER_SOURCE == "open_meteo":
        if selected_station != "BLR":
            st.info("Live Open-Meteo data is fetched for Bengaluru only. Select Bengaluru to see the live wind map.")
        else:
            try:
                with st.spinner("Loading current Open-Meteo conditions…"):
                    current = load_live_current()
                st.plotly_chart(build_animated_wind_figure(current, "Open-Meteo current sample"), use_container_width=True, config={"displayModeBar": False})
                st.caption(f"Animated particle paths are illustrative and driven by the Bengaluru API sample observed {current['observation_timestamp']} Asia/Kolkata. They are not observed streamlines or a measured spatial wind field.")
                m = st.columns(4)
                m[0].metric("Live temperature", f"{current['temperature_c']:.1f} °C")
                m[1].metric("Live humidity", f"{current['humidity_pct']:.0f}%")
                m[2].metric("Live wind", f"{current['wind_speed_kmh']:.1f} km/h")
                m[3].metric("Direction", f"{current['wind_direction_deg']:.0f}°")
            except Exception as exc:
                st.warning("Open-Meteo is temporarily unavailable. Showing no fabricated map data.")
                st.caption(str(exc))
    else:
        rows = readings[(readings["station_id"] == selected_station) & readings["wind_direction_deg"].notna()].copy() if not readings.empty else pd.DataFrame()
        if rows.empty:
            st.info(f"Waiting for the next directional {station_name} sensor event.")
        else:
            sample = rows.sort_values("event_timestamp").iloc[-1].to_dict()
            st.plotly_chart(build_animated_wind_figure(sample, f"Simulated {station_name} sensor", selected_station, station_name),
                            use_container_width=True, config={"displayModeBar": False})
            st.caption(f"Live simulated flow driven by the latest processed {station_name} event: {sample['wind_speed_kmh']:.1f} km/h from {sample['wind_direction_deg']:.0f}°. Particle motion and paths are illustrative; one sensor does not measure a citywide wind field.")
with right:
    st.subheader("Current stations")
    if stations.empty:
        st.info("No processed readings yet. Start the producer and stream processor.")
    else:
        st.dataframe(stations[["station_id","station_name","temperature_c","humidity_pct","wind_speed_kmh","total_readings","anomaly_count"]], use_container_width=True, hide_index=True)

st.subheader("Weather trends · last 60 minutes")
if readings.empty:
    st.info("No readings have been saved yet. Charts will populate when the pipeline inserts events.")
else:
    readings["event_timestamp"] = pd.to_datetime(readings["event_timestamp"], utc=True, errors="coerce")
    choices = sorted(readings["station_id"].dropna().unique())
    sid = selected_station if selected_station in choices else choices[0]
    data = readings[readings.station_id == sid].sort_values("event_timestamp")
    c1,c2,c3 = st.columns(3)
    for col, field, title, color in [(c1,"temperature_c","Temperature · °C","#43d9c3"),(c2,"humidity_pct","Humidity · %","#64a9f5"),(c3,"wind_speed_kmh","Wind speed · km/h","#ffbd69")]:
        with col:
            fig=px.line(data,x="event_timestamp",y=field,color="source_type",title=title, color_discrete_sequence=[color,"#b78cf3"])
            fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",margin={"l":8,"r":8,"t":48,"b":8},legend_title_text="Source")
            fig.update_traces(connectgaps=False)
            st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":False})

st.subheader("Recent readings & anomaly alerts")
if readings.empty:
    st.info("Recent readings will appear here after the first successful database insert.")
else:
    flagged=readings[readings["is_anomaly"].fillna(False).astype(bool) | readings["anomaly_scenario"].notna()]
    if flagged.empty: st.success("No model-flagged anomalies in the last hour.")
    else: st.dataframe(flagged[["event_timestamp","station_id","temperature_c","humidity_pct","wind_speed_kmh","source_type","is_anomaly","anomaly_score","anomaly_scenario"]].head(100),use_container_width=True,hide_index=True)
    st.dataframe(readings[["event_timestamp","station_id","temperature_c","humidity_pct","wind_speed_kmh","source_type","is_anomaly"]].head(30),use_container_width=True,hide_index=True)

st.subheader("Data freshness")
if status.empty or pd.isna(status.iloc[0]["last_insert"]):
    st.info("No database insert has succeeded yet.")
else:
    s=status.iloc[0]
    f=st.columns(3)
    f[0].metric("Last successful DB insert", str(s["last_insert"]))
    f[1].metric("Last processor event", str(s["last_processor_event"]))
    f[2].metric("Last source observation", str(s["last_observation"]))
st.caption("Isolation Forest flags unusual model inputs; it does not issue official weather warnings. Open-Meteo values are API estimates and are not necessarily direct physical-station observations. Streamlines require a spatial wind field, which this single-point API request does not provide.")
time.sleep(DASHBOARD_REFRESH_SECONDS)
st.rerun()
