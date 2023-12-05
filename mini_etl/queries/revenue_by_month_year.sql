-- TODO: This query will return a table with the revenue by month and year. It
-- will have different columns: month_no, with the month numbers going from 01
-- to 12; month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).

SELECT
    strftime('%m',order_date) AS month_no,
    CASE
        WHEN strftime('%m', order_date) = '01' THEN 'Jan'
        WHEN strftime('%m', order_date) = '02' THEN 'Feb'
        WHEN strftime('%m', order_date) = '03' THEN 'Mar'
        WHEN strftime('%m', order_date) = '04' THEN 'Apr'
        WHEN strftime('%m', order_date) = '05' THEN 'May'
        WHEN strftime('%m', order_date) = '06' THEN 'Jun'
        WHEN strftime('%m', order_date) = '07' THEN 'Jul'
        WHEN strftime('%m', order_date) = '08' THEN 'Aug'
        WHEN strftime('%m', order_date) = '09' THEN 'Sep'
        WHEN strftime('%m', order_date) = '10' THEN 'Oct'
        WHEN strftime('%m', order_date) = '11' THEN 'Nov'
        WHEN strftime('%m', order_date) = '12' THEN 'Dec'
    END AS month,
    SUM(CASE WHEN STRFTIME('%Y', order_date) = '2016' THEN aux.value ELSE 0 END) AS Year2016,
    SUM(CASE WHEN STRFTIME('%Y', order_date) = '2017' THEN aux.value ELSE 0 END) AS Year2017,
    SUM(CASE WHEN STRFTIME('%Y', order_date) = '2018' THEN aux.value ELSE 0 END) AS Year2018
FROM(
    SELECT oo.order_delivered_customer_date AS order_date, oop.payment_value AS value
    FROM olist_order_payments oop
    JOIN olist_orders oo ON oo.order_id == oop.order_id
    WHERE oo.order_status = "delivered"
    AND oo.order_delivered_customer_date IS NOT NULL
    GROUP BY oo.order_id
) as aux
GROUP BY month_no;