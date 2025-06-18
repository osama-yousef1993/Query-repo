from os import getenv


from dotenv.main import load_dotenv

load_dotenv()
#
#
# ------------ GCP credentials ---------------
#
#
#
GCP_ACCOUNT_TYPE = getenv("GCP_ACCOUNT_TYPE")
GCP_PROJECT_ID = getenv("GCP_PROJECT_ID")
GCP_PRIVATE_KEY_ID = getenv("GCP_PRIVATE_KEY_ID")
GCP_PRIVATE_KEY = getenv("GCP_PRIVATE_KEY")
GCP_ACCOUNT_EMAIL = getenv("GCP_ACCOUNT_EMAIL")
GCP_ACCOUNT_ID = getenv("GCP_ACCOUNT_ID")
GCP_AUTH_URI = getenv("GCP_AUTH_URI")
GCP_TOKEN_URI = getenv("GCP_TOKEN_URI")
GCP_AUTH_PROVIDER_X509_CERT_URL = getenv("GCP_AUTH_PROVIDER_X509_CERT_URL")
GCP_CLIENT_X509_CERT_URL = getenv("GCP_CLIENT_X509_CERT_URL")
UNIVERSE_DOMAIN = getenv("UNIVERSE_DOMAIN")
