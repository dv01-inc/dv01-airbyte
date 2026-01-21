import json
from typing import Union

from airbyte_cdk import AirbyteLogger
from airbyte_protocol.models import ConfiguredAirbyteStream
from google.cloud.firestore_v1 import FieldFilter
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


from source_google_firestore.FirestoreSource import FirestoreSource


def enable_append_sub_collections(config: json) -> Union[bool, ValueError]:
    append_sub_collections = config["append_sub_collections"]
    if append_sub_collections == "No":
        return False
    elif append_sub_collections == "Yes":
        return True


class QueryHelpers:
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.firestore = firestore
        self.logger = logger
        self.collection_name = airbyte_stream.stream.name
        self.primary_key = config.get("primary_key", "id")
        self.cursor_field = config.get("cursor_field", "updated_at")
        self.append_sub_collections = enable_append_sub_collections(config)

    def get_documents_query(self, document: dict, cursor_value):
        firestore = self.firestore
        cursor_field = self.cursor_field
        base_query = firestore.get_documents(self.collection_name).limit(1000)

        if cursor_value:
            start_after = FieldFilter(cursor_field, ">=", DatetimeWithNanoseconds.fromtimestamp(cursor_value["start_at"]))
            end_before = FieldFilter(cursor_field, "<", DatetimeWithNanoseconds.fromtimestamp(cursor_value["end_at"]))
            base_query = base_query.order_by(cursor_field).where(filter=start_after).where(filter=end_before)
        else:
            base_query = base_query.order_by(self.primary_key)

        if document is not None:
            if cursor_value is None:
                start_after = {self.primary_key: document[self.primary_key]} if document else None
                base_query = base_query.start_after(start_after)
            else:
                start_after = {self.primary_key: document[self.primary_key], self.cursor_field: document.get(self.cursor_field, None)}
                base_query = base_query.start_after(start_after)

        return base_query

    def get_sub_collection_documents(self, parent_id):
        """
        Stream sub-collection documents to avoid loading all into memory at once.
        """
        firestore = self.firestore
        sub_collections_documents = {}
        # Fetch documents from sub-collections
        for sub_collection in firestore.get_sub_collections(self.collection_name, str(parent_id)):
            sub_collection_name = sub_collection.id
            # Stream documents instead of loading all at once
            documents = []
            for child_doc in sub_collection.stream():
                documents.append(child_doc.to_dict())
            sub_collections_documents[sub_collection_name] = documents

        return sub_collections_documents

    def handle_sub_collections(self, parent_documents: list):
        documents = []
        for parent_doc in parent_documents:
            # Fetch nested sub-collections for each parent document
            sub_collections_documents = self.get_sub_collection_documents(parent_doc[self.primary_key])
            documents.append(parent_doc | sub_collections_documents)
        return documents

    def fetch_records(self, cursor_value=None):
        """
        Generator that yields documents in batches to avoid loading all data into memory.
        This prevents memory leaks when dealing with large collections.
        """
        logger = self.logger
        start_at = None
        total_documents = 0

        while True:
            base_query = self.get_documents_query(start_at, cursor_value)
            # Process documents one at a time instead of loading all into list
            documents_batch = []
            for doc in base_query.stream():
                documents_batch.append(doc.to_dict())
            
            if not documents_batch:
                break
            
            if self.append_sub_collections:
                documents_batch = self.handle_sub_collections(documents_batch)
            
            # Yield the batch instead of accumulating
            for doc in documents_batch:
                yield doc
            
            total_documents += len(documents_batch)
            start_at = documents_batch[-1]
            
            logger.info(f"Fetching next batch of documents. Last document: {start_at[self.primary_key]} Total documents processed: {total_documents}")
            
            # Clear batch to free memory
            documents_batch = None
