-- TODO: This query will return a table with the top 10 revenue categories in 
-- English, the number of orders and their total revenue. The first column will 
-- be Category, that will contain the top 10 revenue categories; the second one 
-- will be Num_order, with the total amount of orders of each category; and the 
-- last one will be Revenue, with the total revenue of each catgory.
-- HINT: All orders should have a delivered status and the Category and actual 
-- delivery date should be not null.

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
LIMIT 10;