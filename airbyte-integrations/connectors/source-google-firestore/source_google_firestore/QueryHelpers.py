import json
import time
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

    def get_sub_collection_documents(self, parent_id, max_sub_docs=1000):
        """
        Fetch sub-collection documents for a parent document.
        
        Note: Sub-collections are accumulated in memory per parent document as they need to be 
        merged with the parent. To prevent unbounded memory growth, we limit to max_sub_docs
        per sub-collection.
        
        Args:
            parent_id: The ID of the parent document
            max_sub_docs: Maximum number of documents to fetch per sub-collection (default: 1000)
        
        Returns:
            Dictionary mapping sub-collection names to lists of documents
        """
        firestore = self.firestore
        sub_collections_documents = {}
        
        # Fetch documents from sub-collections
        for sub_collection in firestore.get_sub_collections(self.collection_name, str(parent_id)):
            sub_collection_name = sub_collection.id
            documents = []
            
            # Limit sub-collection size to prevent unbounded memory growth
            for i, child_doc in enumerate(sub_collection.stream()):
                if i >= max_sub_docs:
                    self.logger.warning(
                        f"Sub-collection '{sub_collection_name}' for document '{parent_id}' "
                        f"exceeded {max_sub_docs} documents. Additional documents will be skipped."
                    )
                    break
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

    def fetch_records(self, cursor_value=None, max_retries=5, max_duration_seconds=7200):
        """
        Generator that yields documents one at a time to avoid loading all data into memory.
        This prevents memory leaks when dealing with large collections.
        
        Safety mechanisms:
        - max_retries: Maximum number of pagination attempts (default: 5)
        - max_duration_seconds: Maximum time to spend fetching (default: 7200 seconds / 2 hours)
        
        Args:
            cursor_value: Optional cursor value for incremental syncs
            max_retries: Maximum number of empty batch retries before stopping
            max_duration_seconds: Maximum time in seconds before stopping
        """
        logger = self.logger
        start_at = None
        total_documents = 0
        retry_count = 0
        start_time = time.time()
        
        # Safety check: ensure we have a time limit
        if max_duration_seconds <= 0:
            max_duration_seconds = 7200  # Default to 2 hours
        
        logger.info(f"Starting fetch_records with max_retries={max_retries}, max_duration={max_duration_seconds}s")

        while True:
            # Safety check 1: Check time limit
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_duration_seconds:
                logger.warning(
                    f"Reached maximum time limit of {max_duration_seconds} seconds. "
                    f"Total documents processed: {total_documents}. Stopping gracefully."
                )
                break
            
            # Safety check 2: Check retry limit
            if retry_count >= max_retries:
                logger.warning(
                    f"Reached maximum retry limit of {max_retries} attempts. "
                    f"Total documents processed: {total_documents}. Stopping gracefully."
                )
                break
            
            base_query = self.get_documents_query(start_at, cursor_value)
            # Stream documents one at a time without batching
            document_count = 0
            last_doc = None
            batch_start_time = time.time()
            
            try:
                for doc in base_query.stream():
                    # Safety check 3: Check time limit within batch
                    if time.time() - start_time >= max_duration_seconds:
                        logger.warning(f"Time limit reached during batch processing. Stopping gracefully.")
                        return
                    
                    doc_dict = doc.to_dict()
                    
                    # Verify primary key exists in document
                    if self.primary_key not in doc_dict:
                        logger.warning(f"Document missing primary key '{self.primary_key}', skipping")
                        continue
                    
                    if self.append_sub_collections:
                        # Process sub-collections for this single document
                        sub_collections_documents = self.get_sub_collection_documents(doc_dict[self.primary_key])
                        doc_dict = doc_dict | sub_collections_documents
                    
                    yield doc_dict
                    document_count += 1
                    total_documents += 1
                    last_doc = doc_dict
            except (Exception) as e:
                # Catch Firestore-specific and network errors
                logger.error(f"Error during batch processing: {type(e).__name__}: {str(e)}. Retrying...")
                retry_count += 1
                # Don't continue immediately - let the retry logic below handle it
            
            # If we got documents, reset retry counter
            if document_count > 0:
                retry_count = 0
                batch_duration = time.time() - batch_start_time
                # Safely access primary key with .get() to avoid KeyError
                last_doc_id = last_doc.get(self.primary_key, "unknown") if last_doc else "unknown"
                logger.info(
                    f"Fetched batch of {document_count} documents in {batch_duration:.2f}s. "
                    f"Total documents: {total_documents}. Last document: {last_doc_id}"
                )
                start_at = last_doc
            else:
                # No documents in this batch - increment retry counter
                retry_count += 1
                logger.info(f"Empty batch received. Retry count: {retry_count}/{max_retries}")
                
                # Stop on first empty batch (normal end of collection)
                logger.info(f"No more documents to fetch. Total processed: {total_documents}")
                break
        
        logger.info(f"Completed fetch_records. Total documents processed: {total_documents}")
