import json
from abc import ABC
from datetime import datetime, timedelta
from typing import Mapping, Any

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_protocol.models import ConfiguredAirbyteStream, Type, AirbyteMessage, AirbyteStateMessage

from source_google_firestore.AirbyteHelpers import AirbyteHelpers
from source_google_firestore.FirestoreSource import FirestoreSource
from source_google_firestore.QueryHelpers import QueryHelpers


class FirestoreIncrementalRefresh(IncrementalMixin, ABC):
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.query = QueryHelpers(firestore, logger, config, airbyte_stream)
        self.logger = logger
        self.airbyte = AirbyteHelpers(airbyte_stream, config)
        self.collection_name = airbyte_stream.stream.name
        self.cursor_field = config.get("cursor_field", "updated_at")
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value is not None:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: ''}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value

    def chunk_time(self, last_updated_at):
        timeframes = []
        last_updated_at = datetime.strptime(last_updated_at, "%Y-%m-%dT%H:%M:%S.%f")
        sync_time = datetime.utcnow()
        while last_updated_at < sync_time:
            end_at = last_updated_at + timedelta(minutes=1)
            timeframes.append({"start_at": last_updated_at.timestamp(), "end_at": end_at.timestamp(), self.cursor_field: last_updated_at})
            last_updated_at += timedelta(minutes=1)
        return timeframes

    def get_cursor_value(self, documents: list[dict]):
        max_value = None

        for doc in documents:
            field_value = doc.get(self.cursor_field, None)
            if field_value is not None or field_value != "":
                if max_value is None or field_value > max_value:
                    max_value = field_value
        return max_value

    def stream(self, state):
        airbyte = self.airbyte
        query = self.query

        last_updated_at = state[self.cursor_field] if state[self.cursor_field] else None
        timeframes = self.chunk_time(last_updated_at) if last_updated_at else None

        if timeframes is None or not timeframes:
            # Stream documents one at a time to avoid memory accumulation
            for document in query.fetch_records():
                for airbyte_message in airbyte.send_airbyte_message([document]):
                    yield airbyte_message
            self._cursor_value = datetime.utcnow()
            yield AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=self.state))
        else:
            for timeframe in timeframes:
                self.logger.info(f"Fetching documents from {timeframe['start_at']} to {timeframe['end_at']}")
                # Stream documents one at a time instead of loading all into memory
                for document in query.fetch_records(cursor_value=timeframe):
                    for airbyte_message in airbyte.send_airbyte_message([document]):
                        yield airbyte_message
                self._cursor_value = timeframe[self.cursor_field]
                yield AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=self.state))
