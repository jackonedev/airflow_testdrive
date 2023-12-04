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

    def get_conn(self):
        session = requests.Session()
        session.auth = (MOVIELENS_USER, MOVIELENS_PASSWORD)
        schema = MOVIELENS_SCHEMA
        host = MOVIELENS_HOST
        port = MOVIELENS_PORT
        base_url = f"{schema}://{host}:{port}"
        return session, base_url