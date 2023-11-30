import os
import requests
from dotenv import load_dotenv

load_dotenv()


MOVIELENS_HOST = os.environ.get("MOVIELENS_HOST", "movielens")
MOVIELENS_SCHEMA = os.environ.get("MOVIELENS_SCHEMA", "http")
MOVIELENS_PORT = os.environ.get("MOVIELENS_PORT", "5000")

MOVIELENS_USER = os.environ["MOVIELENS_USER"]
MOVIELENS_PASSWORD = os.environ["MOVIELENS_PASSWORD"]


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
    session, base_url = _get_session()
    # response = session.get(f"{base_url}/ratings")
    # print(response.json())

    # implementing a generator
    ratings = _get_ratings(session, base_url + "/ratings")
    # next(ratings) # this will return the first 100 ratings
    list(ratings) # fetch the entire batch