from abc import ABC, abstractmethod


class BasePersistentClient(ABC):

    def __init__(self, host, port, username, password, database_name):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database_name = database_name
        self._connection = self._connect()

    @abstractmethod
    def _connect(self):
        pass

    @abstractmethod
    def execute_query_one(self):
        pass

    @abstractmethod
    def execute_query_all(self):
        pass

    @abstractmethod
    def insert_one(self):
        pass

    @abstractmethod
    def insert_many(self):
        pass
