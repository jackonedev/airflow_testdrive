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
    
    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        """
        Fetches rating between the given start/end date.
        Parameters
        ————— 
        start_date : str
            Start date to start fetching ratings from (inclusive). Expected
            format is YYYY-MM-DD (equal to Airflow"s ds formats).
        end_date : str
            End date to fetching ratings up to (exclusive). Expected
            format is YYYY-MM-DD (equal to Airflow"s ds formats).
        batch_size : int
            Size of the batches (pages) to fetch from the API. Larger values
            mean less requests, but more data transferred per request.
        """
        yield from self._get_with_pagination(
            endpoint="/ratings",
            params={"start_date": start_date, "end_date": end_date},
            batch_size=batch_size,
        )
        
    def _get_with_pagination(self, endpoint, params, batch_size=100):
        """
        Fetches records using a get request with given url/params,
        taking pagination into account.
        """
        session, base_url = self.get_conn()
        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(base_url, params={
                    **params,
                    **{"offset": offset, "limit": batch_size}
                }
            )
            response.raise_for_status()
            response_json = response.json()
            
            yield from response_json["result"]
            
            offset += batch_size
            total = response_json["total"]