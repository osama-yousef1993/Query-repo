import json

import firebase_admin
from firebase_admin import credentials, firestore
from google.oauth2 import service_account

from src.utils.config import (
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


class GCP:
    def __init__(self):
        self.build_credentials()
        self.cred = credentials.Certificate("credentials.json")
        try:
            self.app = firebase_admin.initialize_app(
                self.cred,
                {
                    "projectId": GCP_PROJECT_ID,
                },
            )
        except ValueError as e:
            self.app = firebase_admin.get_app()
            print(f"error from firestore connection {e}")
        # will use it to get data from collection
        self.db = firestore.client()

    def build_credentials(self):
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
        print(GCP_PRIVATE_KEY)
        with open("credentials.json", "w") as file:
            file.write(json.dumps(credentials_dict))
        _ = service_account.Credentials.from_service_account_file("credentials.json")
        # os.remove("credentials.json")
        # return credential

    def insert_places_data(self, data):
        print(f"data will inserted {len(data)}")
        count = 0
        for item in data:
            try:
                self.db.collection("new_places").document(item["Name"]).set(
                    item, merge=True
                )
                count += 1
            except Exception as e:
                print(f"error from firestore bulk insert process {e} ==> {item}")
        print(f"data inserted {count}")

    def insert_user_data(self, data):
        try:
            resp = self.db.collection("users").document().set(data, merge=True)
            print(resp)
        except Exception as e:
            print(f"error from firestore bulk insert process {e}")

    def get_places_data(self):
        doc_ref = self.db.collection("new_places")
        docs = doc_ref.get()
        return docs

    def get_user_data(self, user_id):
        doc_ref = self.db.collection("users").document(user_id)
        docs = doc_ref.get()
        return docs

    def delete_fs_data(self, doc_id):
        doc_ref = self.db.collection("places").document(doc_id).delete()
        docs = doc_ref.get()
        return docs

    def read_new_users(self, user_id):
        try:
            doc_ref = self.db.collection("users").document(user_id)
            docs = doc_ref.get()
            return docs
        except Exception as e:
            print(e)
            return None
