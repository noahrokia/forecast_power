# src/process.py
import pandas as pd

def load_data(path="data/strava_activities_weather.csv") -> pd.DataFrame:
    """Charge les données enrichies Strava + météo"""
    df = pd.read_csv(path)
    return df

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie et prépare les features pour le ML"""
    # Retenir seulement quelques colonnes utiles
    keep_cols = [
        "distance", "moving_time", "average_speed",
        "total_elevation_gain", "average_watts",
        "temp_c", "wind_kmh", "rel_humidity", "pressure_hpa"
    ]
    df = df[[c for c in keep_cols if c in df.columns]].dropna()

    # Convertir temps en minutes
    df["moving_time_min"] = df["moving_time"] / 60

    # Supprimer les outliers (exemple simple)
    df = df[(df["average_watts"] > 50) & (df["average_watts"] < 500)]

    return df

if __name__ == "__main__":
    df = load_data()
    df_clean = preprocess(df)
    print("✅ Données nettoyées :", df_clean.shape)
    print(df_clean.head())
