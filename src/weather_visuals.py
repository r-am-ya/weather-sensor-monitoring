"""Open-Meteo current conditions and an honest single-point wind map."""
import json
import math
import urllib.parse
import urllib.request
from datetime import datetime

import plotly.graph_objects as go

LATITUDE = 12.9716
LONGITUDE = 77.5946
API_URL = "https://api.open-meteo.com/v1/forecast"


def parse_current_response(payload: dict) -> dict:
    current = payload.get("current")
    if not isinstance(current, dict):
        raise ValueError("Open-Meteo response has no current conditions")
    fields = {
        "temperature_c": "temperature_2m",
        "humidity_pct": "relative_humidity_2m",
        "wind_speed_kmh": "wind_speed_10m",
        "wind_direction_deg": "wind_direction_10m",
        "observation_timestamp": "time",
    }
    values = {out: current.get(source) for out, source in fields.items()}
    for field in ("temperature_c", "humidity_pct", "wind_speed_kmh", "wind_direction_deg"):
        value = values[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"Open-Meteo field {field} is missing or invalid")
    if not isinstance(values["observation_timestamp"], str):
        raise ValueError("Open-Meteo observation time is missing")
    # API local time is explicitly timezone-qualified via its response metadata.
    zone = payload.get("timezone", "Asia/Kolkata")
    try:
        parsed = datetime.fromisoformat(values["observation_timestamp"])
        if parsed.tzinfo is None and zone == "Asia/Kolkata":
            values["observation_timestamp"] = parsed.isoformat() + "+05:30"
    except ValueError as exc:
        raise ValueError("Open-Meteo returned an invalid observation time") from exc
    values["timezone_abbreviation"] = zone
    return values


def fetch_bengaluru_current(timeout: int = 12) -> dict:
    query = urllib.parse.urlencode({
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m",
        "timezone": "Asia/Kolkata",
    })
    request = urllib.request.Request(f"{API_URL}?{query}", headers={"User-Agent": "WeatherPulse/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return parse_current_response(json.loads(response.read().decode("utf-8")))


def build_wind_figure(current: dict) -> go.Figure:
    """Map the sampled Bengaluru vector; no unsampled field is fabricated."""
    direction = math.radians(float(current["wind_direction_deg"]))
    # Meteorological direction is where wind comes from; reverse it for the arrow.
    end_lat = LATITUDE - 0.12 * math.cos(direction)
    end_lon = LONGITUDE - 0.12 * math.sin(direction)
    dx, dy = end_lon - LONGITUDE, end_lat - LATITUDE
    base_lon, base_lat = LONGITUDE + dx * 0.72, LATITUDE + dy * 0.72
    side_lon, side_lat = -dy * 0.22, dx * 0.22
    fig = go.Figure()
    fig.add_trace(go.Scattergeo(
        lon=[LONGITUDE, end_lon], lat=[LATITUDE, end_lat], mode="lines+markers",
        line={"color": "#43d9c3", "width": 5}, marker={"size": [13, 8], "color": [current["wind_speed_kmh"]] * 2, "colorscale": [[0,"#43d9c3"],[1,"#ffbd69"]], "cmin": 0, "cmax": 80, "showscale": True, "colorbar": {"title": {"text": "km/h", "font": {"color": "#d4e5ef"}}, "tickfont": {"color": "#d4e5ef"}}},
        customdata=[[current["temperature_c"], current["humidity_pct"], current["wind_speed_kmh"], current["wind_direction_deg"], current["observation_timestamp"]]] * 2,
        hovertemplate="Bengaluru<br>Temp %{customdata[0]}°C · Humidity %{customdata[1]}%<br>Wind %{customdata[2]} km/h · From %{customdata[3]}°<br>Observed %{customdata[4]}<extra></extra>",
        name="Sampled wind vector",
    ))
    fig.add_trace(go.Scattergeo(
        lon=[end_lon, base_lon + side_lon, base_lon - side_lon, end_lon],
        lat=[end_lat, base_lat + side_lat, base_lat - side_lat, end_lat],
        mode="lines", fill="toself", fillcolor="#43d9c3", line={"color": "#43d9c3"},
        hoverinfo="skip", showlegend=False,
    ))
    fig.update_geos(
        scope="asia", projection_type="mercator", showland=True, landcolor="#132b3c",
        showocean=True, oceancolor="#091823", showcountries=True, countrycolor="#365064",
        lataxis_range=[12.2, 13.8], lonaxis_range=[76.8, 78.4], showcoastlines=False,
    )
    fig.update_layout(height=420, margin={"l": 0, "r": 0, "t": 10, "b": 0},
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      legend={"font": {"color": "#d4e5ef"}})
    return fig


def build_animated_wind_figure(reading: dict, source_label: str) -> go.Figure:
    """Animate illustrative local flow from one real/simulated sensor vector.

    Paths are a uniform-flow visualization around the sampled location, not a
    measured citywide field. The measured speed and direction drive the motion.
    """
    speed = float(reading["wind_speed_kmh"])
    direction_from = float(reading["wind_direction_deg"])
    temperature = float(reading["temperature_c"])
    humidity = float(reading["humidity_pct"])
    observed_at = str(reading.get("observation_timestamp", reading.get("event_timestamp", "—")))
    if not math.isfinite(speed) or speed < 0 or not math.isfinite(direction_from):
        raise ValueError("A finite non-negative speed and direction are required")

    # Convert meteorological direction (where wind comes from) into east/north
    # movement components (where the particles travel).
    angle = math.radians(direction_from)
    east, north = -math.sin(angle), -math.cos(angle)
    cross_east, cross_north = -north, east
    lat0, lon0 = LATITUDE, LONGITUDE
    lon_scale = 111.32 * math.cos(math.radians(lat0))

    def position(along_km: float, across_km: float) -> tuple[float, float]:
        e = east * along_km + cross_east * across_km
        n = north * along_km + cross_north * across_km
        return lon0 + e / lon_scale, lat0 + n / 111.32

    half_length = 17.0
    offsets = [-12.0, -6.0, 0.0, 6.0, 12.0]
    fig = go.Figure()
    for index, offset in enumerate(offsets):
        start_lon, start_lat = position(-half_length, offset)
        end_lon, end_lat = position(half_length, offset)
        fig.add_trace(go.Scattergeo(
            lon=[start_lon, end_lon], lat=[start_lat, end_lat], mode="lines",
            line={"color": "rgba(109,155,176,0.27)", "width": 1.5},
            hoverinfo="skip", showlegend=index == 0,
            name="Illustrative simulated flow paths",
        ))

    particle_trace = len(fig.data)
    particles_per_path = 3
    particle_count = len(offsets) * particles_per_path
    custom = [[speed, direction_from, temperature, humidity, observed_at, source_label]
              for _ in range(particle_count)]

    def particle_positions(phase: float) -> tuple[list[float], list[float]]:
        longitudes, latitudes = [], []
        for path_index, offset in enumerate(offsets):
            for particle_index in range(particles_per_path):
                fraction = ((phase + particle_index / particles_per_path + path_index * 0.071) % 1.0)
                along = (fraction * 2.0 - 1.0) * half_length
                lon, lat = position(along, offset)
                longitudes.append(lon)
                latitudes.append(lat)
        return longitudes, latitudes

    first_lon, first_lat = particle_positions(0.0)
    fig.add_trace(go.Scattergeo(
        lon=first_lon, lat=first_lat, mode="markers",
        marker={"size": max(6, min(15, 5 + speed * 0.28)),
                "color": [speed] * particle_count,
                "colorscale": [[0, "#43d9c3"], [0.5, "#74d7a5"], [1, "#ffbd69"]],
                "cmin": 0, "cmax": 40, "showscale": True,
                "colorbar": {"title": {"text": "Wind · km/h", "font": {"color": "#d4e5ef"}},
                             "tickfont": {"color": "#d4e5ef"}}},
        customdata=custom,
        hovertemplate=("%{customdata[5]}<br>Speed %{customdata[0]:.1f} km/h · "
                       "Direction %{customdata[1]:.0f}°<br>Temperature %{customdata[2]:.1f}°C · "
                       "Humidity %{customdata[3]:.0f}%<br>Observation %{customdata[4]}"
                       "<extra></extra>"),
        name="Animated particles",
    ))
    fig.add_trace(go.Scattergeo(
        lon=[lon0], lat=[lat0], mode="markers+text",
        marker={"size": 14, "color": "#f2fbfd", "line": {"width": 2, "color": "#43d9c3"}},
        text=["BLR"], textposition="top center", textfont={"color": "#f2fbfd"},
        customdata=[[speed, direction_from, temperature, humidity, observed_at]],
        hovertemplate=("Bengaluru sensor<br>Speed %{customdata[0]:.1f} km/h · "
                       "Direction %{customdata[1]:.0f}°<br>Temperature %{customdata[2]:.1f}°C · "
                       "Humidity %{customdata[3]:.0f}%<br>Observation %{customdata[4]}"
                       "<extra></extra>"),
        name="Bengaluru sensor",
    ))

    frame_count = 18
    fig.frames = [go.Frame(
        name=str(frame_index),
        data=[go.Scattergeo(lon=particle_positions(frame_index / frame_count)[0],
                            lat=particle_positions(frame_index / frame_count)[1])],
        traces=[particle_trace],
    ) for frame_index in range(frame_count)]
    duration_ms = max(700, int(4800 - min(speed, 40.0) * 85))
    fig.update_geos(
        scope="asia", projection_type="mercator", showland=True, landcolor="#132b3c",
        showocean=True, oceancolor="#091823", showcountries=True, countrycolor="#365064",
        lataxis_range=[12.65, 13.30], lonaxis_range=[77.25, 77.95], showcoastlines=False,
    )
    fig.update_layout(
        height=470, margin={"l": 0, "r": 0, "t": 6, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend={"font": {"color": "#d4e5ef"}, "bgcolor": "rgba(7,17,27,.55)"},
        updatemenus=[{"type": "buttons", "direction": "left", "x": 0.02, "y": 0.03,
                       "showactive": False, "bgcolor": "#15394a", "font": {"color": "#effafa"},
                       "buttons": [
                           {"label": "▶ Play flow", "method": "animate",
                            "args": [None, {"frame": {"duration": duration_ms, "redraw": True},
                                       "transition": {"duration": 0}, "fromcurrent": True,
                                       "mode": "immediate"}]},
                           {"label": "Ⅱ Pause", "method": "animate",
                            "args": [[None], {"frame": {"duration": 0, "redraw": False},
                                        "mode": "immediate", "transition": {"duration": 0}}]},
                       ]}],
        annotations=[{"text": f"Flow animation · driven by {speed:.1f} km/h at {direction_from:.0f}°",
                      "xref": "paper", "yref": "paper", "x": 0.5, "y": 1.02,
                      "showarrow": False, "font": {"color": "#c6dce5", "size": 12}}],
    )
    return fig
