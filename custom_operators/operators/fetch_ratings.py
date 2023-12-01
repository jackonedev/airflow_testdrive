from airflow.models import BaseOperator, PythonOperator
import logging, json

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from custom_operators.operators.subpackage.extract_ratings import _get_ratings

def _fetch_ratings(templates_dict, batch_size=1000, **_):
    "Extract via HTTP request and save to JSON file. | With logging"
    
    start_date = templates_dict["start_date"]
    end_date = templates_dict["end_date"]
    output_path = templates_dict["output_path"]
    
    logger = logging.getLogger(__name__)
    logger.info(f"Fetching ratings for {start_date} to {end_date}")
    ratings = list(
        _get_ratings(
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
        )
    )
    logger.info(f"Fetched {len(ratings)} ratings")
    logger.info(f"Writing ratings to {output_path}")
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w") as file_:
        json.dump(ratings, fp=file_)


fetch_ratings = PythonOperator(
    task_id="fetch_ratings",
    python_callable=_fetch_ratings,
    templates_dict={
        "start_date": "{{ds}}",
        "end_date": "{{next_ds}}",
        "output_path": "/data/python/ratings/{{ds}}.json",
    },
)
