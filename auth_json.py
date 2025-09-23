# auth_json.py
import os
import json
import time
import requests

TOK_PATH = "tokens.json"  # ajoute 'tokens.json' à ton .gitignore si ce n'est pas déjà fait

def load_tokens():
    if os.path.exists(TOK_PATH):
        with open(TOK_PATH, "r") as f:
            return json.load(f)
    return {"refresh_token": None, "access_token": None, "expires_at": 0}

def save_tokens(d):
    with open(TOK_PATH, "w") as f:
        json.dump(d, f)

def get_access_token(client_id: str, client_secret: str):
    """
    Retourne (access_token, expires_at_epoch).
    Rafraîchit via refresh_token stocké dans tokens.json et met à jour le fichier si rotation.
    """
    d = load_tokens()
    if not d.get("refresh_token"):
        raise RuntimeError("tokens.json manquant ou refresh_token absent")

    # Si un access_token valide existe encore, réutilise-le
    if d.get("access_token") and time.time() < d.get("expires_at", 0) - 60:
        return d["access_token"], d["expires_at"]

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": d["refresh_token"],
    }
    r = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=20)
    tokens = r.json()

    if r.status_code != 200 or "access_token" not in tokens:
        raise RuntimeError(f"Refresh failed: HTTP {r.status_code} | {tokens}")

    d["access_token"] = tokens["access_token"]
    d["expires_at"] = tokens["expires_at"]
    if "refresh_token" in tokens and tokens["refresh_token"]:
        d["refresh_token"] = tokens["refresh_token"]  # rotation éventuelle

    save_tokens(d)
    return d["access_token"], d["expires_at"]
