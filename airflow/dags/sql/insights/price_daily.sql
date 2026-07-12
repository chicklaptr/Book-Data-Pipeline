CREATE OR REPLACE VIEW vw_dm_price_daily AS
SELECT
    DATE(ps.processed_time) AS snapshot_date,
    c.category_id,
    c.category_name,
    COUNT(DISTINCT ps.product_id) AS total_products,
    ROUND(AVG(ps.price)::numeric,2) AS avg_price,
    ROUND(AVG(ps.final_price)::numeric,2) AS avg_final_price,
    ROUND(AVG(ps.discount_percent)::numeric,2) AS avg_discount_percent,
    ROUND(AVG(ps.sold_qty)::numeric,2) AS avg_sold_qty
FROM product_snapshot ps
JOIN product_category_map pcm
ON ps.product_id = pcm.product_id
JOIN categories c
ON pcm.category_id = c.category_id
GROUP BY 
DATE(ps.processed_time),
c.category_id,c.category_name
ORDER BY snapshot_date,
c.category_name;