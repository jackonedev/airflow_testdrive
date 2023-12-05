# -- TODO:
# -- This query will return a table with two columns;
# -- State, and Delivery_Difference.

# -- The first one will have the letters that identify the states.

# -- The second one the average difference between the estimate delivery
# -- date and the date when the items were actually delivered to the customer.

# -- HINTS:
# -- 1. You can use the julianday function to convert a date to a number.
# -- 2. You can use the CAST function to convert a number to an integer.
# -- 3. You can use the STRFTIME function to convert a order_delivered_customer_date
# --    to a string removing hours, minutes and seconds.
# -- 4. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL

import sqlite3, os
import pandas as pd

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sqlite_file = os.path.join(project_root, "olist.db")

con = sqlite3.connect(sqlite_file)

sql_query = """SELECT 
    State,
    CEIL(AVG(Delivery_Difference)) AS Delivery_Difference
FROM (
    SELECT 
    	State,
        CAST((julianday(order_estimated_delivery_date) - julianday(order_delivered_customer_date)) AS INTEGER)
        AS Delivery_Difference
    FROM (
        SELECT 
            order_status,
            order_estimated_delivery_date,
            order_delivered_customer_date,
            customer_state AS State
        FROM olist_orders
        INNER JOIN olist_customers USING(customer_id)
        WHERE order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
    )
)
GROUP BY State
ORDER BY Delivery_Difference ASC"""

df = pd.read_sql(sql_query, con)

df
