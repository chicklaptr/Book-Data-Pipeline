CREATE OR REPLACE VIEW vw_dm_category_performance AS
WITH latest_snapshot AS (
    SELECT ps.*
    FROM product_snapshot ps
    JOIN (
        SELECT product_id, MAX(processed_time) AS product_processed_time
        FROM product_snapshot
        GROUP BY product_id
    ) latest
        ON ps.product_id = latest.product_id
       AND ps.processed_time = latest.product_processed_time
)
SELECT
    c.category_id,
    c.category_name,
    COUNT(DISTINCT pcm.product_id) AS total_products,
    ROUND(AVG(ls.price)::numeric, 2) AS avg_price,
    ROUND(AVG(ls.final_price)::numeric, 2) AS avg_final_price,
    ROUND(AVG(ls.discount_percent)::numeric, 2) AS avg_discount_percent,
    COUNT(DISTINCT CASE WHEN ls.stock_available = TRUE THEN p.product_id END) AS in_stock_products,
    COUNT(DISTINCT CASE WHEN ls.stock_available = FALSE THEN p.product_id END) AS out_stock_products,
    ROUND(AVG(ls.sold_qty)::numeric, 2) AS avg_sold_qty
FROM categories c
LEFT JOIN product_category_map pcm
    ON c.category_id = pcm.category_id
LEFT JOIN products p
    ON pcm.product_id = p.product_id
LEFT JOIN latest_snapshot ls
    ON p.product_id = ls.product_id
GROUP BY
    c.category_id,
    c.category_name;