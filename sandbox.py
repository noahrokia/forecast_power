# sandbox.py
import datetime
from auth_json import get_access_token

CLIENT_ID = "178262"
CLIENT_SECRET = "TON_NOUVEAU_CLIENT_SECRET"

token, exp = get_access_token(CLIENT_ID, CLIENT_SECRET)
print("✅ token:", token[:12], "...")      # ne pas afficher en entier
print("🚦 expire:", datetime.datetime.fromtimestamp(exp))
