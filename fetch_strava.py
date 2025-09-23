import os
import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from auth_json import get_access_token
from logger import get_logger  # 👈 import de ton module logger

# Charger client_id / secret depuis .env
load_dotenv()
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")

# Créer le logger
logger = get_logger(__name__)

def fetch_activities(access_token, per_page=50, pages=1):
    headers = {"Authorization": f"Bearer {access_token}"}
    all_activities = []
    for page in range(1, pages + 1):
        url = f"https://www.strava.com/api/v3/athlete/activities?per_page={per_page}&page={page}"
        logger.debug("Fetching page %s: %s", page, url)  # 👈 log debug
        try:
            res = requests.get(url, headers=headers, timeout=20)
            res.raise_for_status()  # lève une erreur si HTTP != 200
            data = res.json()
            logger.info("Page %s récupérée (%s activités)", page, len(data))
            all_activities.extend(data)
        except requests.RequestException as e:
            logger.error("Erreur lors de la récupération de la page %s: %s", page, e)
    return all_activities

def save_to_csv(activities, filename="data/strava_activities.csv"):
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(activities)
    df.to_csv(filename, index=False)
    logger.info("✅ %s activités sauvegardées dans %s", len(df), filename)  # 👈 log info

if __name__ == "__main__":
    access_token, exp = get_access_token(CLIENT_ID, CLIENT_SECRET)
    logger.info("✅ Access token actif jusqu’à %s",
                datetime.datetime.fromtimestamp(exp))

    activities = fetch_activities(access_token, per_page=50, pages=3)
    save_to_csv(activities)
