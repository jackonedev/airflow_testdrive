import sqlite3, os
import pandas as pd


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sqlite_file = os.path.join(project_root, "olist.db")

con = sqlite3.connect(sqlite_file)

sql_query = """
SELECT 
    oc.customer_state AS customer_state,
    SUM(oop.payment_value) AS Revenue
FROM 
    olist_orders oo
    INNER JOIN olist_order_payments oop ON oo.order_id = oop.order_id
    INNER JOIN olist_customers oc ON oo.customer_id = oc.customer_id
WHERE 
    oo.order_status = 'delivered'
    AND oo.order_delivered_customer_date IS NOT NULL
GROUP BY 
    oc.customer_state
ORDER BY 
    Revenue DESC
LIMIT 10; -- limit to top 10 states with most revenue

-- This query retrieves the customer_state and the total revenue for each state. It joins the olist_orders, olist_order_payments, and olist_customers tables using the appropriate keys. The WHERE clause filters for orders with a delivered status and a non-null delivery date. The results are then grouped by customer_state and ordered by revenue in descending order. Finally, the LIMIT clause is used to limit the results to the top 10 states with the most revenue.
"""


df = pd.read_sql(sql_query, con)

df
