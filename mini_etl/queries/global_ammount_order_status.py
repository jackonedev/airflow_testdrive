import sqlite3, os
import pandas as pd

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sqlite_file = os.path.join(project_root, "olist.db")

con = sqlite3.connect(sqlite_file)


sql_query = """
SELECT oo.order_status, COUNT(oo.order_status) AS Ammount
FROM olist_orders oo
GROUP BY oo.order_status
"""

df = pd.read_sql(sql_query, con)

df
