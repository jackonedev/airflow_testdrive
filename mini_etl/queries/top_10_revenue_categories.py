import sqlite3, os
import pandas as pd


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sqlite_file = os.path.join(project_root, "olist.db")

con = sqlite3.connect(sqlite_file)


sql_query = """
SELECT
    pcnt.product_category_name_english AS Category,
    COUNT(DISTINCT oo.order_id) AS Num_order,
    SUM(oop.payment_value) AS Revenue
FROM
    olist_orders oo
    INNER JOIN olist_order_items ooi ON oo.order_id = ooi.order_id
    INNER JOIN olist_products op ON ooi.product_id = op.product_id
    INNER JOIN olist_order_payments oop ON oo.order_id = oop.order_id
    INNER JOIN product_category_name_translation pcnt ON op.product_category_name = pcnt.product_category_name
WHERE
    oo.order_status = 'delivered'
AND
    oo.order_delivered_customer_date IS NOT NULL

GROUP BY
    op.product_category_name
ORDER BY
    Revenue DESC
LIMIT 10; -- limit to top 10 revenue categories
"""


df = pd.read_sql(sql_query, con)
df

