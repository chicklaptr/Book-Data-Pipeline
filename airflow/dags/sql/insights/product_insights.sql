CREATE OR REPLACE VIEW vw_latest_product_snapshot AS
WITH latest_snapshot AS(
    SELECT 
    ps.*,
    ROW_NUMBER() OVER(
        PARTITION BY ps.product_id
        ORDER BY ps.processed_time DESC
    ) as rn
    FROM product_snapshot ps
)
SELECT
    p.product_id,
    p.product_name,
    p.product_url,
    p.image_src,
    ls.price,
    ls.final_price,
    ls.discount_percent,
    ls.stock_available,
    ls.sold_qty,
    ls.processed_time
FROM latest_snapshot ls
JOIN products p
    ON ls.product_id = p.product_id 
WHERE ls.rn =1;

CREATE OR REPLACE VIEW vw_top_discount_products AS 
SELECT 
    product_id,
    product_name,
    price,
    final_price,
    discount_percent,
    processed_time
FROM vw_latest_product_snapshot
WHERE discount_percent is not NULL
ORDER BY discount_percent DESC, processed_time DESC;

CREATE OR REPLACE VIEW vw_top_sold_products AS 
SELECT 
    product_id,
    product_name,
    sold_qty,
    final_price,
    discount_percent,
    processed_time
FROM vw_latest_product_snapshot 
WHERE sold_qty is NOT NULL
ORDER BY sold_qty DESC, processed_time DESC;



