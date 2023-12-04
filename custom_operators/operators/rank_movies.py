from airflow.models import BaseOperator, PythonOperator
import pandas as pd

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from custom_operators.operators.subpackage.rank_movies import rank_movies_by_rating

def _rank_movies(templates_dict, min_ratings=2, **_):
    input_path = templates_dict["input_path"]
    output_path = templates_dict["output_path"]
    ratings = pd.read_json(input_path)
    ranking = rank_movies_by_rating(ratings, min_ratings=min_ratings)
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    ranking.to_csv(output_path, index=True)

rank_movies = PythonOperator(
    task_id="rank_movies",
    python_callable=_rank_movies,
    templates_dict={
        "input_path": "/data/python/ratings/{{ds}}.json",
        "output_path": "/data/python/rankings/{{ds}}.csv",
    },
)