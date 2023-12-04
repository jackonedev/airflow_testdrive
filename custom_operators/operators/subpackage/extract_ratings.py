import requests
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from utils.config import MOVIELENS_USER, MOVIELENS_PASSWORD
from utils.config import MOVIELENS_HOST, MOVIELENS_SCHEMA, MOVIELENS_PORT


def _get_session() -> tuple:
    """Builds a requests Session for the Movielens API."""
    session = requests.Session()
    session.auth = (MOVIELENS_USER, MOVIELENS_PASSWORD)
    base_url = f"{MOVIELENS_SCHEMA}://{MOVIELENS_HOST}:{MOVIELENS_PORT}"
    return session, base_url


def _get_with_pagination(session, url, params, batch_size=100):
    """
    Fetches records using a GET request with given URL/params,
    taking pagination into account.
    """
    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            url,
            params={
                **params,
                **{"offset": offset, "limit": batch_size}
            }
        )
        response.raise_for_status()
        response_json = response.json()
        yield from response_json["result"]
        offset += batch_size
        total = response_json["total"]


def _get_ratings(start_date, end_date, batch_size=100):
    session, base_url = _get_session()
    yield from _get_with_pagination(
        session=session,
        url=base_url + "/ratings",
        params={"start_date": start_date, "end_date": end_date},
        batch_size=batch_size,
    )


if __name__ == "__main__":
    # print(MOVIELENS_USER)
    session, base_url = _get_session()
    # response = session.get(f"{base_url}/ratings")
    # print(response.json())

    # implementing a generator
    ratings = _get_ratings(session, base_url + "/ratings")
    # next(ratings) # this will return the first 100 ratings
    print(list(ratings)) # fetch the entire batch