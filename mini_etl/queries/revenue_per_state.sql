-- TODO: This query will return a table with two columns; customer_state, and 
-- Revenue. The first one will have the letters that identify the top 10 states 
-- with most revenue and the second one the total revenue of each.
-- HINT: All orders should have a delivered status and the actual delivery date 
-- should be not null. 
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

-- This query retrieves the customer_state and the total revenue for each state.
-- It joins the olist_orders, olist_order_payments, and olist_customers tables 
-- using the appropriate keys. The WHERE clause filters for orders with a delivered
-- status and a non-null delivery date. The results are then grouped by customer_state
-- and ordered by revenue in descending order. Finally, the LIMIT clause is used 
-- to limit the results to the top 10 states with the most revenue.