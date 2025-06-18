import json

import firebase_admin
import google.cloud.exceptions
from firebase_admin import credentials, firestore
from google.oauth2 import service_account

from src_.utils.config import (
    GCP_ACCOUNT_EMAIL,
    GCP_ACCOUNT_ID,
    GCP_ACCOUNT_TYPE,
    GCP_AUTH_PROVIDER_X509_CERT_URL,
    GCP_AUTH_URI,
    GCP_CLIENT_X509_CERT_URL,
    GCP_PRIVATE_KEY,
    GCP_PRIVATE_KEY_ID,
    GCP_PROJECT_ID,
    GCP_TOKEN_URI,
    UNIVERSE_DOMAIN,
)


class GCPCloud:
    def __init__(self):
        self.build_credential()
        self.cred = credentials.Certificate("credentials.json")
        try:
            self.app = firebase_admin.initialize_app(
                self.cred, {"projectId": GCP_PROJECT_ID}
            )
        except Exception as e:
            self.app = firebase_admin.get_app()
            print(e)
        self.db = firestore.client()

    def build_credential(self):
        credentials_dict = {
            "type": GCP_ACCOUNT_TYPE,
            "project_id": GCP_PROJECT_ID,
            "private_key": GCP_PRIVATE_KEY,
            "private_key_id": GCP_PRIVATE_KEY_ID,
            "client_id": GCP_ACCOUNT_ID,
            "client_email": GCP_ACCOUNT_EMAIL,
            "auth_uri": GCP_AUTH_URI,
            "token_uri": GCP_TOKEN_URI,
            "auth_provider_x509_cert_url": GCP_AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": GCP_CLIENT_X509_CERT_URL,
            "universe_domain": UNIVERSE_DOMAIN,
        }
        # credentials_dict["private_key"] = credentials_dict["private_key"].replace(
        #     "\\n", "\n"
        # )
        with open("credentials.json", "w") as file:
            file.write(json.dumps(credentials_dict))
        auth_result = service_account.Credentials.from_service_account_file(
            "credentials.json"
        )
        print(auth_result)

    def get_user_data(self, user_id):
        try:
            user_doc_ref = self.db.collection("users").document(user_id)
            user_doc = user_doc_ref.get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                return user_data
            else:
                print("User Not Found")
                return None
        except google.cloud.exceptions.NotFound:
            print("Error: Document not found")
            return None
        except Exception as e:
            print("Error occurred:", e)
            return None

    def get_trip_data(self, trip_id):
        try:
            trip_doc_ref = self.db.collection("trips").document(trip_id)
            trip_doc = trip_doc_ref.get()
            if trip_doc.exists:
                trip_data = trip_doc.to_dict()
                return trip_data
            else:
                print("Trip Not Found")
                return None

        except google.cloud.exceptions.NotFound:
            print("Error: Document not found")
            return None
        except Exception as e:
            print("Error occurred:", e)
            return None

    def get_destination_data(self, des_id):
        try:
            destination_doc_ref = self.db.collection("destinations").document(des_id)
            destination_doc = destination_doc_ref.get()
            if destination_doc.exists:
                destination_data = destination_doc.to_dict()
                return destination_data
            else:
                print("Destination Not Found")
                return None
        except google.cloud.exceptions.NotFound:
            print("Error: Document not found")
            return None
        except Exception as e:
            print("Error occurred:", e)
            return None

    def get_destinations(self):
        try:
            destinations_doc_refs = self.db.collection("destinations")
            destinations_docs = destinations_doc_refs.get()
            destinations_data = list()
            for doc in destinations_docs:
                destinations_data.append(doc.to_dict())
            return destinations_data
        except google.cloud.exceptions.NotFound:
            print("Error: Document not found")
            return None
        except Exception as e:
            print("Error occurred:", e)
            return None
