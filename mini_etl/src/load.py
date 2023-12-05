from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine) -> None:
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
    """
    # For each dataframe in the dictionary, you must:
    for table_name, dataframe in data_frames.items():
        # use pandas.DataFrame.to_sql() to load the dataframe into the database as a
        # table.
        dataframe.to_sql(table_name, database, index=False)
