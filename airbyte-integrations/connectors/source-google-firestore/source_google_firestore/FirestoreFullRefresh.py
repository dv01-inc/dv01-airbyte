import json

from airbyte_cdk import AirbyteLogger
from airbyte_protocol.models import ConfiguredAirbyteStream

from source_google_firestore.AirbyteHelpers import AirbyteHelpers
from source_google_firestore.FirestoreSource import FirestoreSource
from source_google_firestore.QueryHelpers import QueryHelpers


class FirestoreFullRefresh:
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.query = QueryHelpers(firestore, logger, config, airbyte_stream)
        self.airbyte = AirbyteHelpers(airbyte_stream, config)
        self.logger = logger

    def stream(self):
        """
        Stream documents to avoid loading all into memory.
        Yields Airbyte messages one at a time.
        """
        total_documents = 0
        # Use generator to avoid loading all documents into memory
        for document in self.query.fetch_records():
            total_documents += 1
            # Yield messages one at a time
            for message in self.airbyte.send_airbyte_message([document]):
                yield message
        
        self.logger.info(f"Finished streaming documents. Total documents: {total_documents}")
