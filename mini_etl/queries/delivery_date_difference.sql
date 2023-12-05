SELECT
    State,
    CEIL(AVG(Delivery_Difference)) AS Delivery_Difference
FROM (
    SELECT 
    	State,
        CAST(julianday(order_estimated_delivery_date) - julianday(order_delivered_customer_date) AS INTEGER)
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
ORDER BY Delivery_Difference ASC
