from airflow.hooks.base import BaseHook
import requests

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.config import MOVIELENS_USER, MOVIELENS_PASSWORD
from utils.config import MOVIELENS_HOST, MOVIELENS_SCHEMA, MOVIELENS_PORT

class MovieLensHook(BaseHook):
    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self.DEFAULT_SCHEMA = MOVIELENS_SCHEMA
        self.DEFAULT_HOST = MOVIELENS_HOST
        self.DEFAULT_PORT = MOVIELENS_PORT
        self._session = None
        self._base_url = None
        
    def get_conn(self):
        if self._session is None:
            config = self.get_connection(self._conn_id)
            session = requests.Session()
            session.auth = (MOVIELENS_USER, MOVIELENS_PASSWORD)
            schema = config.schema or self.DEFAULT_SCHEMA
            host = config.host or self.DEFAULT_HOST
            port = config.port or self.DEFAULT_PORT
            base_url = f"{schema}://{host}:{port}"
            self._session = session
            self._base_url = base_url
        
        return self._session, self._base_url