import os
import dotenv
dotenv.load_dotenv()

# --- CONFIG ---
API_BASE_URL = "https://acleddata.com/api/acled/read"
TOKEN_URL = "https://acleddata.com/oauth/token"

USERNAME = os.getenv("ACLED_USERNAME")
PASSWORD = os.getenv("ACLED_PASSWORD")
