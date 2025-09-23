import os
import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from auth_json import get_access_token

# Charger client_id / secret depuis .env
load_dotenv()
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")

def fetch_activities(access_token, per_page=50, pages=1):
    headers = {"Authorization": f"Bearer {access_token}"}
    all_activities = []
    for page in range(1, pages + 1):
        url = f"https://www.strava.com/api/v3/athlete/activities?per_page={per_page}&page={page}"
        res = requests.get(url, headers=headers, timeout=20)
        data = res.json()
        all_activities.extend(data)
    return all_activities

def save_to_csv(activities, filename="data/strava_activities.csv"):
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(activities)
    df.to_csv(filename, index=False)
    print(f"✅ {len(df)} activités sauvegardées dans {filename}")

if __name__ == "__main__":
    access_token, exp = get_access_token(CLIENT_ID, CLIENT_SECRET)
    print("✅ Access token actif jusqu’à :", datetime.datetime.fromtimestamp(exp))

    activities = fetch_activities(access_token, per_page=50, pages=3)
    save_to_csv(activities)
