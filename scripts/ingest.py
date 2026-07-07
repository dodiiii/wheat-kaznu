#!/usr/bin/env python3
"""
Wheat Field IoT — Data Ingest Pipeline
=======================================
Open-Meteo  →  CSV  →  InfluxDB 2.x

Fetches real weather & soil data from Open-Meteo Historical API,
calculates growth metrics (GDD, BBCH stage, NDVI, yield forecast),
generates realistic pest/disease and equipment events correlated
with the real weather, and loads everything into InfluxDB.

Usage:
    pip install -r requirements.txt
    python ingest.py            # fetch + load
    python ingest.py --csv-only # fetch + save CSV, skip InfluxDB
"""

import argparse
import csv
import math
import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ─── Configuration ────────────────────────────────────────────────────────────

FIELDS = [
    {"id": "field_01", "name": "Поле Северное",    "lat": 52.05, "lon": 63.60, "area_ha": 120},
    {"id": "field_02", "name": "Поле Восточное",   "lat": 52.10, "lon": 63.75, "area_ha":  95},
    {"id": "field_03", "name": "Поле Центральное", "lat": 52.00, "lon": 63.65, "area_ha": 150},
    {"id": "field_04", "name": "Поле Южное",       "lat": 51.95, "lon": 63.70, "area_ha": 110},
    {"id": "field_05", "name": "Поле Западное",    "lat": 52.08, "lon": 63.55, "area_ha": 130},
]

SEASON_START = "2024-05-01"
SEASON_END   = "2024-09-30"

INFLUXDB_URL    = os.getenv("INFLUXDB_URL",    "http://localhost:8086")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN",  "wheat-monitoring-token-2024")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG",    "wheat-farm")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "wheat_monitoring")

METEO_BASE = "https://archive-api.open-meteo.com/v1/archive"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Wheat agronomic constants
T_BASE_WHEAT = 0.0          # Base temperature for spring wheat GDD (°C)
GDD_MATURITY = 1800.0       # Approx GDD to reach full maturity

# BBCH growth stages for spring wheat (cumulative GDD thresholds)
BBCH_STAGES = [
    (0,    "00-09 Germination"),
    (120,  "10-19 Leaf development"),
    (350,  "20-29 Tillering"),
    (600,  "30-39 Stem elongation"),
    (900,  "40-49 Booting"),
    (1050, "50-59 Heading"),
    (1200, "60-69 Flowering"),
    (1450, "70-79 Grain filling"),
    (1700, "80-89 Ripening"),
    (1800, "90-99 Maturity"),
]

# Soil statics per field (based on SoilGrids typical values for Kostanay region)
SOIL_STATICS = {
    "field_01": {"ph": 7.2, "organic_carbon": 18.5, "nitrogen": 1.8, "sand": 42, "silt": 35, "clay": 23},
    "field_02": {"ph": 7.0, "organic_carbon": 21.0, "nitrogen": 2.1, "sand": 38, "silt": 37, "clay": 25},
    "field_03": {"ph": 7.4, "organic_carbon": 16.0, "nitrogen": 1.5, "sand": 45, "silt": 33, "clay": 22},
    "field_04": {"ph": 6.8, "organic_carbon": 23.5, "nitrogen": 2.4, "sand": 35, "silt": 38, "clay": 27},
    "field_05": {"ph": 7.1, "organic_carbon": 19.2, "nitrogen": 1.9, "sand": 40, "silt": 36, "clay": 24},
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def daterange(start_str, end_str):
    """Yield date strings YYYY-MM-DD."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    d = start
    while d <= end:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def bbch_stage(gdd_cum):
    """Return BBCH stage name for a given cumulative GDD."""
    stage = BBCH_STAGES[0][1]
    for threshold, name in BBCH_STAGES:
        if gdd_cum >= threshold:
            stage = name
    return stage


def ndvi_from_gdd(gdd_cum):
    """Synthetic NDVI curve based on GDD accumulation."""
    # Logistic rise → plateau → decline
    if gdd_cum < 50:
        return 0.15 + random.gauss(0, 0.02)
    elif gdd_cum < 1100:
        # Rising sigmoid
        x = (gdd_cum - 50) / 1050.0
        v = 0.15 + 0.65 * (1 / (1 + math.exp(-10 * (x - 0.4))))
        return min(0.88, v + random.gauss(0, 0.02))
    else:
        # Decline after flowering
        x = (gdd_cum - 1100) / 700.0
        v = 0.80 - 0.55 * min(1.0, x)
        return max(0.12, v + random.gauss(0, 0.02))


def yield_forecast_from_gdd(gdd_cum, precip_cum, field):
    """Simple empirical yield forecast (t/ha)."""
    # Potential yield modulated by water & heat accumulation
    base = 2.8 + hash(field["id"]) % 5 * 0.3
    gdd_factor  = min(1.0, gdd_cum / GDD_MATURITY)
    water_factor = min(1.0, precip_cum / 250.0)  # 250 mm is ~adequate
    return round(base * gdd_factor * water_factor, 2)


# ─── Step 1: Fetch weather + soil from Open-Meteo ────────────────────────────

def fetch_openmeteo(field):
    """Fetch daily weather + hourly soil data and return unified daily dict list."""
    print(f"  ⛅ Fetching Open-Meteo data for {field['name']} ({field['lat']}, {field['lon']})…")

    daily_vars = [
        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
        "precipitation_sum", "wind_speed_10m_max",
        "shortwave_radiation_sum", "et0_fao_evapotranspiration",
    ]
    hourly_vars = [
        "relative_humidity_2m",
        "soil_temperature_0cm", "soil_temperature_6cm",
        "soil_temperature_18cm", "soil_temperature_54cm",
        "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm",
        "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm",
        "soil_moisture_27_to_81cm",
    ]

    params = {
        "latitude":   field["lat"],
        "longitude":  field["lon"],
        "start_date": SEASON_START,
        "end_date":   SEASON_END,
        "daily":      ",".join(daily_vars),
        "hourly":     ",".join(hourly_vars),
        "timezone":   "auto",
    }

    for attempt in range(3):
        try:
            resp = requests.get(METEO_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            print(f"    ⚠ Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                print("    ✗ Giving up on this field")
                return None
            time.sleep(2 ** attempt)

    # --- Parse daily data ---
    daily = data.get("daily", {})
    dates = daily.get("time", [])

    weather_rows = []
    for i, date in enumerate(dates):
        row = {
            "date": date,
            "temperature_max":  daily.get("temperature_2m_max",  [None]*len(dates))[i],
            "temperature_min":  daily.get("temperature_2m_min",  [None]*len(dates))[i],
            "temperature_mean": daily.get("temperature_2m_mean", [None]*len(dates))[i],
            "precipitation":    daily.get("precipitation_sum",   [None]*len(dates))[i],
            "wind_speed":       daily.get("wind_speed_10m_max",  [None]*len(dates))[i],
            "solar_radiation":  daily.get("shortwave_radiation_sum", [None]*len(dates))[i],
            "evapotranspiration": daily.get("et0_fao_evapotranspiration", [None]*len(dates))[i],
        }
        weather_rows.append(row)

    # --- Parse hourly data → aggregate to daily means ---
    hourly = data.get("hourly", {})
    hourly_times = hourly.get("time", [])

    # Group hourly indices by date
    date_hours = defaultdict(list)
    for idx, ts in enumerate(hourly_times):
        dt = ts[:10]  # YYYY-MM-DD
        date_hours[dt].append(idx)

    soil_rows = []
    for date in dates:
        indices = date_hours.get(date, [])
        if not indices:
            continue

        def hmean(var_name):
            vals = [hourly.get(var_name, [None]*len(hourly_times))[j] for j in indices]
            vals = [v for v in vals if v is not None]
            return round(sum(vals) / len(vals), 3) if vals else None

        rh = hmean("relative_humidity_2m")
        # Soil temp: 0-7cm = avg(0cm, 6cm), 7-28cm = 18cm, 28-100cm = 54cm
        st_0   = hmean("soil_temperature_0cm")
        st_6   = hmean("soil_temperature_6cm")
        st_18  = hmean("soil_temperature_18cm")
        st_54  = hmean("soil_temperature_54cm")

        soil_temp_0_7   = round((st_0 + st_6) / 2, 2)   if (st_0 is not None and st_6 is not None)  else st_0
        soil_temp_7_28  = st_18
        soil_temp_28_100 = st_54

        # Soil moisture (m³/m³): weighted average for 0-7cm
        sm_0_1 = hmean("soil_moisture_0_to_1cm")
        sm_1_3 = hmean("soil_moisture_1_to_3cm")
        sm_3_9 = hmean("soil_moisture_3_to_9cm")

        if sm_0_1 is not None and sm_1_3 is not None and sm_3_9 is not None:
            soil_moist_0_7 = round((sm_0_1 * 1 + sm_1_3 * 2 + sm_3_9 * 4) / 7, 4)
        else:
            soil_moist_0_7 = sm_3_9

        soil_moist_7_28  = hmean("soil_moisture_9_to_27cm")
        soil_moist_28_100 = hmean("soil_moisture_27_to_81cm")

        soil_rows.append({
            "date": date,
            "humidity": rh,
            "soil_temp_0_7":      soil_temp_0_7,
            "soil_temp_7_28":     soil_temp_7_28,
            "soil_temp_28_100":   soil_temp_28_100,
            "soil_moist_0_7":     soil_moist_0_7,
            "soil_moist_7_28":    soil_moist_7_28,
            "soil_moist_28_100":  soil_moist_28_100,
        })

    # Merge weather + soil by date
    soil_by_date = {r["date"]: r for r in soil_rows}
    merged = []
    for wr in weather_rows:
        sr = soil_by_date.get(wr["date"], {})
        merged.append({**wr, **sr})

    # --- Fallback: synthesize soil data if Open-Meteo returned None ---
    has_soil = any(r.get("soil_temp_0_7") is not None for r in merged)
    if not has_soil:
        print("    ⚠ No ERA5 soil data — synthesizing from weather (damped wave model)")
        random.seed(hash(field["id"]) + 77)
        # Cumulative precipitation for moisture model
        precip_cum = 0.0
        soil_moist = [0.30, 0.33, 0.35]  # initial moisture at 3 depths (m³/m³)
        for i, row in enumerate(merged):
            t_air = row.get("temperature_mean", 15) or 15
            precip = row.get("precipitation", 0) or 0
            et = row.get("evapotranspiration", 3) or 3
            day_of_season = i  # 0-based

            # Soil temperature: damped & lagged version of air temp
            # Deeper layers: more damping, more lag
            for depth_idx, (d_label, damp, lag_days) in enumerate([
                ("soil_temp_0_7",    0.85, 1),
                ("soil_temp_7_28",   0.60, 5),
                ("soil_temp_28_100", 0.35, 15),
            ]):
                ref_i = max(0, i - lag_days)
                ref_t = merged[ref_i].get("temperature_mean", 15) or 15
                soil_t = 8.0 + damp * (ref_t - 8.0) + random.gauss(0, 0.3)
                row[d_label] = round(soil_t, 2)

            # Soil moisture: simple bucket model
            # Infiltration from precipitation, loss from ET
            precip_cum += precip
            for depth_idx, (m_label, et_frac, drain_rate) in enumerate([
                ("soil_moist_0_7",    0.60, 0.03),
                ("soil_moist_7_28",   0.25, 0.01),
                ("soil_moist_28_100", 0.10, 0.005),
            ]):
                infiltration = precip * [0.7, 0.2, 0.1][depth_idx] / 100.0  # mm→m³/m³ approx
                loss = et * et_frac / 100.0
                drainage = drain_rate * soil_moist[depth_idx]
                soil_moist[depth_idx] += infiltration - loss - drainage
                soil_moist[depth_idx] = max(0.08, min(0.45, soil_moist[depth_idx]))
                soil_moist[depth_idx] += random.gauss(0, 0.003)
                row[m_label] = round(max(0.05, min(0.48, soil_moist[depth_idx])), 4)

    print(f"    ✓ Got {len(merged)} daily records")
    return merged


# ─── Step 2: Calculate growth metrics ────────────────────────────────────────

def calc_growth(weather_data, field):
    """Calculate GDD, BBCH stage, NDVI, yield forecast from weather."""
    random.seed(hash(field["id"]))
    rows = []
    gdd_cum = 0.0
    precip_cum = 0.0

    for rec in weather_data:
        t_mean = rec.get("temperature_mean")
        precip  = rec.get("precipitation", 0) or 0
        if t_mean is None:
            t_mean = 15.0  # fallback

        gdd_daily = max(0, t_mean - T_BASE_WHEAT)
        gdd_cum  += gdd_daily
        precip_cum += precip

        stage    = bbch_stage(gdd_cum)
        ndvi     = round(ndvi_from_gdd(gdd_cum), 3)
        yld      = yield_forecast_from_gdd(gdd_cum, precip_cum, field)

        rows.append({
            "date":            rec["date"],
            "gdd_daily":       round(gdd_daily, 1),
            "gdd_cumulative":  round(gdd_cum, 1),
            "growth_stage":    stage,
            "ndvi":            ndvi,
            "yield_forecast":  yld,
        })

    return rows


# ─── Step 3: Generate pest/disease data ──────────────────────────────────────

PEST_TYPES = [
    "leaf_rust",     # Бурая ржавчина (Puccinia triticina)
    "septoria",      # Септориоз (Septoria tritici)
    "thrips",        # Пшеничный трипс (Haplothrips tritici)
    "grain_moth",    # Серая зерновая совка (Hadena sordida)
    "flea_beetle"    # Хлебная блошка (Phyllotreta vittula)
]

def gen_pest_disease(weather_data, growth_data, field):
    """Generate pest/disease events correlated with weather & growth stage."""
    random.seed(hash(field["id"]) + 42)
    rows = []

    for wr, gr in zip(weather_data, growth_data):
        date  = wr["date"]
        t_mean = wr.get("temperature_mean", 15)
        rh     = wr.get("humidity", 60) or 60
        precip = wr.get("precipitation", 0) or 0
        gdd    = gr["gdd_cumulative"]

        # Base weather-related probability of pest/disease pressure
        base_prob = 0.05
        if rh > 70:
            base_prob += 0.08
        if 15 < t_mean < 28:
            base_prob += 0.05
        if precip > 5:
            base_prob += 0.04

        for pest in PEST_TYPES:
            pest_thresh = base_prob
            
            # 1. Хлебная блошка - активна в теплые дни в начале вегетации (всходы, GDD < 450)
            if pest == "flea_beetle":
                if gdd < 450:
                    pest_thresh += 0.15 if t_mean > 16 else 0.05
                else:
                    pest_thresh = 0.01 # Spends summer elsewhere
            
            # 2. Пшеничный трипс - атакует колос при колошении/цветении (GDD 700 - 1100, жара)
            elif pest == "thrips":
                if 700 <= gdd <= 1100:
                    pest_thresh += 0.18 if t_mean > 20 else 0.05
                else:
                    pest_thresh = 0.01
            
            # 3. Серая зерновая совка - активна при созревании зерна (GDD > 1200, теплая сухая погода)
            elif pest == "grain_moth":
                if gdd > 1200:
                    pest_thresh += 0.20 if (t_mean > 18 and rh < 65) else 0.05
                else:
                    pest_thresh = 0.01

            # 4. Бурая ржавчина - требует высокой влажности и тепла (GDD > 600)
            elif pest == "leaf_rust":
                if gdd > 600 and rh > 80 and t_mean > 18:
                    pest_thresh += 0.18
                else:
                    pest_thresh = max(0.01, pest_thresh - 0.05)

            # 5. Септориоз - активируется частыми осадками во время цветения и налива (GDD > 800)
            elif pest == "septoria":
                if gdd > 800 and precip > 4 and rh > 75:
                    pest_thresh += 0.22
                else:
                    pest_thresh = max(0.02, pest_thresh - 0.03)

            if random.random() < pest_thresh:
                severity = min(10, max(1, int(random.gauss(
                    3 + 5.5 * (pest_thresh - 0.05), 1.2
                ))))
                weed_cov = round(random.uniform(1.5, 22.0), 1)
                outbreak = 1 if severity >= 6 else 0

                rows.append({
                    "date":          date,
                    "pest_type":     pest,
                    "severity":      severity,
                    "weed_coverage": weed_cov,
                    "outbreak":      outbreak,
                })

    return rows


# ─── Step 4: Generate equipment tracking data ────────────────────────────────

OPERATIONS = [
    {"name": "seeding",      "start_day": 0,   "duration": 5,  "speed_range": (6, 10)},
    {"name": "fertilizing",  "start_day": 20,  "duration": 3,  "speed_range": (8, 12)},
    {"name": "spraying",     "start_day": 45,  "duration": 2,  "speed_range": (7, 11)},
    {"name": "spraying_2",   "start_day": 75,  "duration": 2,  "speed_range": (7, 11)},
    {"name": "harvesting",   "start_day": 135, "duration": 7,  "speed_range": (4, 8)},
]

MACHINES = ["tractor_01", "tractor_02", "sprayer_01", "combine_01"]

def gen_equipment(field):
    """Generate equipment tracks with GPS positions for field operations."""
    random.seed(hash(field["id"]) + 99)
    rows = []
    season_start = datetime.strptime(SEASON_START, "%Y-%m-%d")
    lat0, lon0 = field["lat"], field["lon"]
    area = field["area_ha"]

    # Approximate field as ~rectangle: side ≈ sqrt(area * 10000) meters
    side_m = math.sqrt(area * 10000)
    dlat = side_m / 111320.0  # degrees lat per meter
    dlon = side_m / (111320.0 * math.cos(math.radians(lat0)))

    for op in OPERATIONS:
        op_start = season_start + timedelta(days=op["start_day"])
        machine = random.choice(MACHINES)
        if op["name"] == "harvesting":
            machine = "combine_01"
        elif "spraying" in op["name"]:
            machine = "sprayer_01"

        for day_offset in range(op["duration"]):
            cur_date = op_start + timedelta(days=day_offset)
            if cur_date > datetime.strptime(SEASON_END, "%Y-%m-%d"):
                break

            # Generate track points (every 30 min for ~10h working day)
            n_points = 20
            for pt in range(n_points):
                # Simulate back-and-forth path across the field
                progress = (day_offset * n_points + pt) / (op["duration"] * n_points)
                row_pct = (pt / n_points)

                track_lat = lat0 + dlat * (progress - 0.5) + random.gauss(0, dlat * 0.01)
                track_lon = lon0 + dlon * (row_pct - 0.5)  + random.gauss(0, dlon * 0.01)

                speed = round(random.uniform(*op["speed_range"]), 1)
                # Status transitions
                if pt == 0:
                    status = "starting"
                elif pt == n_points - 1:
                    status = "finishing"
                elif random.random() < 0.05:
                    status = "idle"
                else:
                    status = "working"

                ts = cur_date + timedelta(minutes=30 * pt + random.randint(0, 10))
                area_covered = round(area * progress, 1)

                rows.append({
                    "timestamp":    ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "date":         cur_date.strftime("%Y-%m-%d"),
                    "machine_id":   machine,
                    "operation":    op["name"].replace("_2", ""),
                    "lat":          round(track_lat, 6),
                    "lon":          round(track_lon, 6),
                    "speed":        speed,
                    "status":       status,
                    "area_covered": area_covered,
                })

    return rows


# ─── Step 4b: Generate agronomy insights & recommendations ───────────────────

def gen_agronomy_insights(weather_data, growth_data, pest_data, field):
    """Generate daily agronomic action insights and risk assessment."""
    random.seed(hash(field["id"]) + 7)
    rows = []
    
    # Map pest_data by date for fast lookup
    pest_lookup = {}
    for row in pest_data:
        dt = row["date"]
        if dt not in pest_lookup:
            pest_lookup[dt] = []
        pest_lookup[dt].append(row)
        
    for wr, gr in zip(weather_data, growth_data):
        date = wr["date"]
        gdd = gr["gdd_cumulative"]
        stage = gr["growth_stage"]
        ndvi = gr["ndvi"]
        
        # Get soil moisture at 0-7 cm
        m_val = wr.get("soil_moist_0_7", 0.25)
        
        # Determine risk level, recommendation, insights
        risk = "info"
        rec = "Плановый мониторинг"
        insight = "Состояние посевов стабильное, продолжается плановый мониторинг вегетации пшеницы."
        soil_status = f"Влажность почвы оптимальная ({int(m_val*100)}%)."
        pest_status = "Активности вредителей не обнаружено."
        
        # Pests for this day
        active_pests = pest_lookup.get(date, [])
        max_severity = 0
        worst_pest = None
        for p in active_pests:
            if p["severity"] > max_severity:
                max_severity = p["severity"]
                worst_pest = p["pest_type"]
                
        # Phase wise defaults and actions
        if "Всходы" in stage or "3-й лист" in stage:
            rec = "Оценка всходов"
            insight = "Период прорастания пшеницы. Оцените густоту стояния растений. Проверьте признаки присутствия хлебной блошки на листьях."
            if max_severity >= 5 and worst_pest == "flea_beetle":
                rec = "Обработка от блошки 🛑"
                insight = f"Внимание! Обнаружена хлебная блошка высокой плотности (тяжесть {max_severity}/10) на всходах. Рекомендуется обработка инсектицидом по краю поля."
                risk = "warning"
        elif "Кущение" in stage:
            rec = "Азотная подкормка"
            insight = "Активная фаза кущения пшеницы (BBCH 21-29). Оптимальный период для внесения азотных удобрений (КАС, аммиачная селитра) для кустистости."
            if m_val < 0.16:
                rec = "Отложить удобрения ⚠️"
                insight = "Риск засухи! Верхний слой почвы сухой. Внесение сухих удобрений неэффективно, перенесите подкормку до выпадения осадков."
                risk = "warning"
        elif "Выход в трубку" in stage:
            rec = "Экстремальный щит (гербицид)"
            insight = "Период трубкования зерновых. Проведите фитосанитарный обход, оцените засоренность сорняками. Рекомендуется гербицидная защита."
            if max_severity >= 5 and worst_pest == "leaf_rust":
                rec = "Внесение фунгицидов 🛑"
                insight = f"Обнаружена бурая ржавчина листьев (тяжесть {max_severity}/10). Риск быстрого перезаражения стебля. Срочно обработать фунгицидом."
                risk = "critical"
        elif "Колошение" in stage or "Цветение" in stage:
            rec = "Защита колоса"
            insight = "Колошение и цветение пшеницы. Критический период водопотребления. Исключите пестицидные обработки в пик цветения."
            if max_severity >= 5 and worst_pest == "thrips":
                rec = "Инсектицид от трипса 🛑"
                insight = f"Критическое превышение порога вредоносности пшеничного трипса (тяжесть {max_severity}/10). Опрыскать системным инсектицидом."
                risk = "critical"
            elif max_severity >= 5 and worst_pest == "septoria":
                rec = "Фунгицид от септориоза 🛑"
                insight = f"Дожди во время колошения спровоцировали септориоз пшеницы (тяжесть {max_severity}/10). Срочно проведите фунгицидную обработку колоса."
                risk = "critical"
        elif "Налив зерна" in stage:
            rec = "Мониторинг вредителей колоса"
            insight = "Фаза налива зерна (молочно-восковая спелость). Проводите регулярные кошения сачком для выявления личинок совки."
            if max_severity >= 5 and worst_pest == "grain_moth":
                rec = "Дезинсекция совки 🛑"
                insight = f"Критическая опасность! Серая зерновая совка повреждает созревающее зерно (тяжесть {max_severity}/10). Срочно внести инсектициды."
                risk = "critical"
        elif "Созревание" in stage:
            rec = "Подготовка к жатве"
            insight = "Фаза полной спелости. Подготовка комбайнов к уборочной кампании. Замеряйте влажность зерна (норма 14%)."
            if wr.get("precipitation", 0) > 8:
                rec = "Приостановить уборку ⚠️"
                insight = "Осадки во время спелости. Уборочную кампанию приостановить до полного высыхания стеблестоя во избежание потерь и порчи зерна."
                risk = "warning"

        # Soil moisture triggers
        if m_val < 0.12:
            soil_status = f"🚨 Критическая засуха! Влажность {int(m_val*100)}%"
            risk = "critical"
            rec = "Орошение / Антистрессанты 🚨"
            insight = "Влажность почвы упала до критического минимума (суховей). Рост пшеницы угнетен. Срочно применить полив или антистрессовые аминокислоты."
        elif m_val < 0.17:
            soil_status = f"⚠️ Дефицит влаги ({int(m_val*100)}%)"
            if risk == "info":
                risk = "warning"
                rec = "Агроприемы влагосбережения"
                insight = "Сухая почва. Приостановите глубокое рыхление и механические обработки для удержания остаточной влаги."

        # Pest status string formulation
        if max_severity > 0:
            pest_names = {
                "leaf_rust": "Бурая ржавчина",
                "septoria": "Септориоз",
                "thrips": "Пшеничный трипс",
                "grain_moth": "Серая зерновая совка",
                "flea_beetle": "Хлебная блошка"
            }
            pname = pest_names.get(worst_pest, worst_pest)
            pest_status = f"Выявлен {pname} (тяжесть {max_severity}/10)."
            if max_severity >= 6:
                pest_status = f"🚨 Вспышка! {pname} превысил ЭПВ ({max_severity}/10)."

        rows.append({
            "date": date,
            "recommendation": rec,
            "insight": insight,
            "risk_level": risk,
            "soil_status": soil_status,
            "pest_status": pest_status
        })

    return rows


# ─── Step 5: Save CSVs ───────────────────────────────────────────────────────

def save_csvs(all_data):
    """Save all data to CSV files."""
    ensure_dir(DATA_DIR)

    # Weather CSV
    with open(os.path.join(DATA_DIR, "weather.csv"), "w", newline="") as f:
        writer = None
        for fid, data in all_data.items():
            for row in data["weather"]:
                out = {"field_id": fid, **row}
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=out.keys())
                    writer.writeheader()
                writer.writerow(out)
    print(f"  📄 Saved weather.csv")

    # Soil static CSV
    with open(os.path.join(DATA_DIR, "soil_static.csv"), "w", newline="") as f:
        writer = None
        for fid, props in SOIL_STATICS.items():
            out = {"field_id": fid, **props}
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=out.keys())
                writer.writeheader()
            writer.writerow(out)
    print(f"  📄 Saved soil_static.csv")

    # Growth CSV
    with open(os.path.join(DATA_DIR, "growth.csv"), "w", newline="") as f:
        writer = None
        for fid, data in all_data.items():
            for row in data["growth"]:
                out = {"field_id": fid, **row}
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=out.keys())
                    writer.writeheader()
                writer.writerow(out)
    print(f"  📄 Saved growth.csv")

    # Pest/disease CSV
    with open(os.path.join(DATA_DIR, "pest_disease.csv"), "w", newline="") as f:
        writer = None
        for fid, data in all_data.items():
            for row in data["pest"]:
                out = {"field_id": fid, **row}
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=out.keys())
                    writer.writeheader()
                writer.writerow(out)
    print(f"  📄 Saved pest_disease.csv")

    # Equipment CSV
    with open(os.path.join(DATA_DIR, "equipment.csv"), "w", newline="") as f:
        writer = None
        for fid, data in all_data.items():
            for row in data["equipment"]:
                out = {"field_id": fid, **row}
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=out.keys())
                    writer.writeheader()
                writer.writerow(out)
    print(f"  📄 Saved equipment.csv")

    # Agronomy insights CSV
    with open(os.path.join(DATA_DIR, "agronomy_insights.csv"), "w", newline="") as f:
        writer = None
        for fid, data in all_data.items():
            if "insights" in data:
                for row in data["insights"]:
                    out = {"field_id": fid, **row}
                    if writer is None:
                        writer = csv.DictWriter(f, fieldnames=out.keys())
                        writer.writeheader()
                    writer.writerow(out)
    print(f"  📄 Saved agronomy_insights.csv")


# ─── Step 6: Load into InfluxDB ──────────────────────────────────────────────

def load_influxdb(all_data):
    """Load all datasets into InfluxDB."""
    print("\n🔄 Loading data into InfluxDB…")
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for fid, data in all_data.items():
        field = next(f for f in FIELDS if f["id"] == fid)
        print(f"  📤 Loading {field['name']}…")

        # --- Weather ---
        points = []
        for row in data["weather"]:
            ts = datetime.strptime(row["date"], "%Y-%m-%d")
            p = (Point("weather")
                 .tag("field_id", fid)
                 .tag("field_name", field["name"])
                 .tag("lat", str(field["lat"]))
                 .tag("lon", str(field["lon"]))
                 .time(ts, WritePrecision.S))
            if row.get("temperature_max") is not None:
                p = p.field("temperature_max", float(row["temperature_max"]))
            if row.get("temperature_min") is not None:
                p = p.field("temperature_min", float(row["temperature_min"]))
            if row.get("temperature_mean") is not None:
                p = p.field("temperature_mean", float(row["temperature_mean"]))
            if row.get("humidity") is not None:
                p = p.field("humidity", float(row["humidity"]))
            if row.get("precipitation") is not None:
                p = p.field("precipitation", float(row["precipitation"]))
            if row.get("wind_speed") is not None:
                p = p.field("wind_speed", float(row["wind_speed"]))
            if row.get("solar_radiation") is not None:
                p = p.field("solar_radiation", float(row["solar_radiation"]))
            if row.get("evapotranspiration") is not None:
                p = p.field("evapotranspiration", float(row["evapotranspiration"]))
            points.append(p)
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"    ✓ weather: {len(points)} points")

        # --- Soil dynamic (3 depth layers) ---
        points = []
        for row in data["weather"]:  # soil data merged into weather rows
            ts = datetime.strptime(row["date"], "%Y-%m-%d")
            for depth, t_key, m_key in [
                ("0_7cm",   "soil_temp_0_7",    "soil_moist_0_7"),
                ("7_28cm",  "soil_temp_7_28",   "soil_moist_7_28"),
                ("28_100cm","soil_temp_28_100",  "soil_moist_28_100"),
            ]:
                t_val = row.get(t_key)
                m_val = row.get(m_key)
                if t_val is None and m_val is None:
                    continue
                p = (Point("soil_dynamic")
                     .tag("field_id", fid)
                     .tag("depth", depth)
                     .time(ts, WritePrecision.S))
                if t_val is not None:
                    p = p.field("temperature", float(t_val))
                if m_val is not None:
                    p = p.field("moisture", float(m_val))
                points.append(p)
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"    ✓ soil_dynamic: {len(points)} points")

        # --- Soil static ---
        ss = SOIL_STATICS.get(fid, {})
        if ss:
            ts = datetime.strptime(SEASON_START, "%Y-%m-%d")
            p = (Point("soil_static")
                 .tag("field_id", fid)
                 .time(ts, WritePrecision.S)
                 .field("ph",              float(ss["ph"]))
                 .field("organic_carbon",  float(ss["organic_carbon"]))
                 .field("nitrogen",        float(ss["nitrogen"]))
                 .field("sand_pct",        float(ss["sand"]))
                 .field("silt_pct",        float(ss["silt"]))
                 .field("clay_pct",        float(ss["clay"])))
            write_api.write(bucket=INFLUXDB_BUCKET, record=p)
            print(f"    ✓ soil_static: 1 point")

        # --- Growth ---
        points = []
        for row in data["growth"]:
            ts = datetime.strptime(row["date"], "%Y-%m-%d")
            p = (Point("growth")
                 .tag("field_id", fid)
                 .time(ts, WritePrecision.S)
                 .field("gdd_daily",       float(row["gdd_daily"]))
                 .field("gdd_cumulative",  float(row["gdd_cumulative"]))
                 .field("growth_stage",    row["growth_stage"])
                 .field("ndvi",            float(row["ndvi"]))
                 .field("yield_forecast",  float(row["yield_forecast"])))
            points.append(p)
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"    ✓ growth: {len(points)} points")

        # --- Pest / disease ---
        points = []
        for row in data["pest"]:
            ts = datetime.strptime(row["date"], "%Y-%m-%d")
            p = (Point("pest_disease")
                 .tag("field_id", fid)
                 .tag("pest_type", row["pest_type"])
                 .time(ts, WritePrecision.S)
                 .field("severity",      int(row["severity"]))
                 .field("weed_coverage", float(row["weed_coverage"]))
                 .field("outbreak",      int(row["outbreak"])))
            points.append(p)
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"    ✓ pest_disease: {len(points)} points")

        # --- Equipment ---
        points = []
        for row in data["equipment"]:
            ts = datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
            p = (Point("equipment")
                 .tag("field_id", fid)
                 .tag("machine_id", row["machine_id"])
                 .tag("operation", row["operation"])
                 .tag("status", row["status"])
                 .time(ts, WritePrecision.S)
                 .field("lat",           float(row["lat"]))
                 .field("lon",           float(row["lon"]))
                 .field("speed",         float(row["speed"]))
                 .field("area_covered",  float(row["area_covered"])))
            points.append(p)
        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
        print(f"    ✓ equipment: {len(points)} points")

        # --- Agronomy insights ---
        if "insights" in data:
            points = []
            for row in data["insights"]:
                ts = datetime.strptime(row["date"], "%Y-%m-%d")
                p = (Point("agronomy_insights")
                     .tag("field_id", fid)
                     .time(ts, WritePrecision.S)
                     .field("recommendation", row["recommendation"])
                     .field("insight",        row["insight"])
                     .field("risk_level",     row["risk_level"])
                     .field("soil_status",    row["soil_status"])
                     .field("pest_status",    row["pest_status"]))
                points.append(p)
            write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            print(f"    ✓ agronomy_insights: {len(points)} points")

    client.close()
    print("✅ All data loaded into InfluxDB")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Wheat Field IoT Data Ingest")
    parser.add_argument("--csv-only", action="store_true",
                        help="Save CSV files only, skip InfluxDB loading")
    args = parser.parse_args()

    print("🌾 Wheat Field IoT Data Ingest Pipeline")
    print(f"   Fields:  {len(FIELDS)}")
    print(f"   InfluxDB: {INFLUXDB_URL}\n")

    all_data = {}
    
    seasons = [
        ("2021-05-01", "2021-09-30"),
        ("2022-05-01", "2022-09-30"),
        ("2023-05-01", "2023-09-30"),
        ("2024-05-01", "2024-09-30"),
        ("2025-05-01", "2025-09-30"),
    ]

    global SEASON_START, SEASON_END

    for field in FIELDS:
        print(f"\n{'='*60}")
        print(f"  🌾 Processing {field['name']} (ID: {field['id']})")
        print(f"{'='*60}")

        field_weather = []
        field_growth = []
        field_pest = []
        field_equip = []
        field_insights = []

        for s_start, s_end in seasons:
            print(f"  📅 Season: {s_start} → {s_end}")
            SEASON_START = s_start
            SEASON_END = s_end

            # 1. Fetch weather & soil from Open-Meteo
            weather = fetch_openmeteo(field)
            if weather is None:
                print(f"    ⚠ Skipping season {s_start} — no data")
                continue

            # 2. Calculate growth metrics
            growth = calc_growth(weather, field)
            print(f"      ✓ Growth: {len(growth)} records, final GDD={growth[-1]['gdd_cumulative']}")

            # 3. Generate pest/disease
            pest = gen_pest_disease(weather, growth, field)
            print(f"      ✓ Pest/disease: {len(pest)} events")

            # 4. Generate equipment
            equip = gen_equipment(field)
            print(f"      ✓ Equipment: {len(equip)} track points")

            # 4b. Generate agronomic insights
            insights = gen_agronomy_insights(weather, growth, pest, field)
            print(f"      ✓ Agronomy insights: {len(insights)} daily records")

            # Combine
            field_weather.extend(weather)
            field_growth.extend(growth)
            field_pest.extend(pest)
            field_equip.extend(equip)
            field_insights.extend(insights)

            # Be nice to Open-Meteo (free API)
            time.sleep(1)

        all_data[field["id"]] = {
            "weather":   field_weather,
            "growth":    field_growth,
            "pest":      field_pest,
            "equipment": field_equip,
            "insights":  field_insights,
        }

    # Save CSVs
    print(f"\n{'='*60}")
    print("📁 Saving CSV files…")
    save_csvs(all_data)

    # Load to InfluxDB
    if not args.csv_only:
        load_influxdb(all_data)
    else:
        print("\n⏩ Skipping InfluxDB load (--csv-only mode)")

    print("\n🎉 Done!")


if __name__ == "__main__":
    main()
