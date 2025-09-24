import os
import math
import pandas as pd
from datetime import timezone
from meteostat import Hourly, Point

from logger import get_logger  # <- ton logger réutilisable
from datetime import datetime
from zoneinfo import ZoneInfo


log = get_logger(__name__)  # niveau contrôlable via LOG_LEVEL=DEBUG

INPUT = "data/strava_activities.csv"
OUTPUT = "data/strava_activities_weather.csv"

# Cache simple pour éviter des appels répétés à la même (lat,lon,heure)
_cache = {}

def round_coord(x, nd=3):
    return None if pd.isna(x) else round(float(x), nd)

def parse_start_latlng_col(df: pd.DataFrame) -> pd.DataFrame:
    """Crée start_lat / start_lng à partir de start_latlng (si nécessaire)."""
    if "start_lat" in df.columns and "start_lng" in df.columns:
        return df
    if "start_latlng" not in df.columns:
        raise ValueError("Colonnes start_lat/start_lng ou start_latlng absentes du CSV")

    def parse_latlng(x):
        try:
            if isinstance(x, str):
                x = x.strip("[]")
                lat, lon = x.split(",")
                return float(lat), float(lon)
            if isinstance(x, (list, tuple)) and len(x) == 2:
                return float(x[0]), float(x[1])
        except Exception:
            return (math.nan, math.nan)
        return (math.nan, math.nan)

    latlon = df["start_latlng"].apply(parse_latlng).tolist()
    df["start_lat"] = [t[0] for t in latlon]
    df["start_lng"] = [t[1] for t in latlon]
    return df

from datetime import timezone
import pandas as pd
from meteostat import Hourly, Point

def get_hourly_weather(lat: float, lon: float, when_utc) -> dict | None:
    """
    Retourne la météo horaire pour (lat, lon, when_utc).
    Accepte when_utc en string/Timestamp tz-aware (UTC) ou tz-naive.
    On convertit tout en UTC NAIVE (sans tz) car Meteostat le gère mieux.
    """
    # 1) Parse en Timestamp
    ts = pd.to_datetime(when_utc, errors="coerce")
    if ts is None or pd.isna(ts):
        log.debug(f"Timestamp invalide: {when_utc}")
        return None

    # 2) Si aware -> convertit en UTC puis enlève la timezone
    if ts.tzinfo is not None and ts.tzinfo.utcoffset(ts) is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    else:
        # ts est naive -> on **suppose** qu'il est déjà en UTC
        ts = ts.tz_localize(None)

    # 3) Arrondir à l'heure (Meteostat est horaire) SANS remettre de tz
    ts = ts.replace(minute=0, second=0, microsecond=0)

    key = (round_coord(lat), round_coord(lon), ts)
    if key in _cache:
        return _cache[key]

    try:
        loc = Point(lat, lon)
        df_wx = Hourly(loc, ts, ts).fetch()
        if df_wx.empty:
            log.debug(f"Aucune donnée meteostat pour {key}")
            _cache[key] = None
            return None

        row = df_wx.iloc[0]
        wx = {
            "weather_time_utc": row.name.to_pydatetime().isoformat(),
            "temp_c": row.get("temp"),
            "precip_mm": row.get("prcp"),
            "wind_kmh": row.get("wspd"),
            "wind_dir_deg": row.get("wdir"),
            "rel_humidity": row.get("rhum"),
            "pressure_hpa": row.get("pres"),
            "snow_mm": row.get("snow") if "snow" in df_wx.columns else None,
        }
        _cache[key] = wx
        return wx
    except Exception:
        log.exception(f"Erreur Meteostat pour lat={lat}, lon={lon}, when={ts}")
        return None


def main():
    if not os.path.exists(INPUT):
        log.error(f"Fichier introuvable: {INPUT}")
        return

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    log.info(f"Lecture des activités: {INPUT}")
    df = pd.read_csv(INPUT)

    # Optionnel: ne garder que le vélo
    if "type" in df.columns:
        before = len(df)
        df = df[df["type"].isin(["Ride", "VirtualRide"])].copy()
        log.info(f"Filtrage vélo: {before} -> {len(df)}")

    # Colonnes lat/lon
    try:
        df = parse_start_latlng_col(df)
    except ValueError as e:
        log.error(str(e))
        return

    df = df.dropna(subset=["start_lat", "start_lng"])
    if df.empty:
        log.warning("Aucune activité avec coordonnées valides.")
        return

    # Colonne temporelle
    time_col = "start_date" if "start_date" in df.columns else "start_date_local" if "start_date_local" in df.columns else None
    if not time_col:
        log.error("Colonne start_date ou start_date_local absente.")
        return

    # Parser en datetime (UTC si start_date, sinon convertir)
    utc_flag = (time_col == "start_date")
    df[time_col] = pd.to_datetime(df[time_col], utc=utc_flag, errors="coerce")
    df = df.dropna(subset=[time_col])

    # Enrichissement
    log.info(f"Début enrichissement météo sur {len(df)} activités…")
    rows, misses = [], 0
    for i, r in df.iterrows():
        lat, lon = float(r["start_lat"]), float(r["start_lng"])
        ts = r[time_col]
        when_utc = ts if utc_flag else ts.tz_convert("UTC")

        wx = get_hourly_weather(lat, lon, when_utc)
        if wx is None:
            misses += 1
            rows.append({**r.to_dict(),
                         "weather_time_utc": None,
                         "temp_c": None, "precip_mm": None,
                         "wind_kmh": None, "wind_dir_deg": None,
                         "rel_humidity": None, "pressure_hpa": None,
                         "snow_mm": None})
        else:
            rows.append({**r.to_dict(), **wx})

        if (len(rows) % 50) == 0:
            log.debug(f"Progression: {len(rows)}/{len(df)}")

    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT, index=False)
    log.info(f"✅ Enrichi: {len(out)} activités → {OUTPUT} (météo manquante: {misses})")
    log.debug(f"Cache Meteostat taille: {len(_cache)}")

if __name__ == "__main__":
    main()
