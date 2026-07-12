CREATE OR REPLACE VIEW vw_category_product_counts AS
SELECT  
    c.category_id,
    c.category_name,
    COUNT(DISTINCT pcm.product_id) AS total_products
FROM categories c
JOIN product_category_map pcm
    ON c.category_id=pcm.category_id
GROUP BY c.category_id, c.category_name
ORDER BY total_products DESC;
