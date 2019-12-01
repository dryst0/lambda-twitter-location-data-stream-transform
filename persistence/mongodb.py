
from pymongo import MongoClient

from .base import BasePersistentClient


class MongoDBPersistentClient(BasePersistentClient):
    def __init__(self, host, port, username, password, database_name, client_name='pymongo_client', auth_source='admin', tz_aware=True, read_preference='primaryPreferred', read_concern='majority', write_concern=1, journal=True, uuid_representation='standard'):
        super().__init__(host, port, username, password, database_name)
        self._client_name = client_name
        self._auth_source = auth_source
        self._tz_aware = tz_aware
        self._read_preference = read_preference
        self._read_concern = read_concern
        self._write_concern = write_concern
        self._journal = journal
        self._uuid_representation = uuid_representation

    def _connect(self):
        return MongoClient(
            host=self._host,
            port=self._port,
            username=self._userame,
            password=self._password,
            appname=self._client_name,
            authSource=self._auth_source,
            readPreference=self._read_preference,
            readConcernLevel=self._read_concern,
            w=self._write_concern,
            journal=self._journal,
            uuidRepresentation=self._uuid_representation,
            connect=True
        )[self.database_name]

    def execute_query_one(self, collection_name, **kwargs):
        return self._connection[collection_name].find_one(**kwargs)

    def execute_query_all(self, collection_name, **kwargs):
        result = self._connection[collection_name].find(**kwargs)

        return list(result)

    def execute_aggregate_query(self, collection_name, pipeline):
        result = self._connection[collection_name].aggregate(pipeline)

        return list(result)

    def insert_one(self, collection_name, document, **kwargs):
        result = self._connection[collection_name].insert_one(document, **kwargs)

        return result.inserted_id

    def insert_many(self, collection_name, list_of_documents, **kwargs):
        result = self._connection[collection_name].insert_many(list_of_documents, **kwargs)

        return result.inserted_ids
