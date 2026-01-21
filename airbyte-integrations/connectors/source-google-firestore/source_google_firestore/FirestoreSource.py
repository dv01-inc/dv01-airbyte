#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Optional
from google.cloud import firestore
from google.oauth2 import service_account


class FirestoreSource:
    def __init__(self, project_id: str, credentials_json: Optional[str] = None):
        connection = {"project": project_id}

        if credentials_json:
            try:
                json_account_info = json.loads(credentials_json, strict=False)
            except ValueError:
                raise ValueError("The 'credentials_json' field must contain a valid JSON document with service account access data.")

            credentials = service_account.Credentials.from_service_account_info(json_account_info)
            connection["credentials"] = credentials

        self.client = firestore.Client(**connection)

    def check(self) -> bool:
        return bool(self.collections())

    def collections(self) -> list:
        return list(self.client.collections())

    def get_documents(self, name: str):
        return self.client.collection(name)

    def get_sub_collections(self, collection_name: str, document_id: str):
        return self.client.collection(collection_name).document(document_id).collections()
    
    def close(self):
        """Close the Firestore client to release resources."""
        if self.client:
            self.client.close()
