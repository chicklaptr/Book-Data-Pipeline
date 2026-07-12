CREATE OR REPLACE VIEW vw_pipeline_quality_runs AS
SELECT 
    pr.process_run_id,
    pr.processed_at,
    pr.source_name,
    pr.raw_file_minio,
    pr.total_raw_records,
    dqr.total_products_seen,
    dqr.products_passed,
    dqr.products_failed,
    dqr.missing_product_id,
    dqr.missing_product_name,
    dqr.missing_product_url,
    dqr.invalid_price_negative,
    dqr.invalid_final_price_negative,
    dqr.final_price_greater_than_price,
    dqr.duplicate_product_id
FROM pipeline_runs pr
JOIN data_quality_reports dqr
    ON pr.process_run_id = dqr.process_run_id;

CREATE OR REPLACE VIEW vw_pipeline_failure_rate AS
SELECT  
    pr.process_run_id,
    pr.processed_at,
    dqr.total_products_seen,
    dqr.products_failed,
    ROUND(100.0 * dqr.products_failed / NULLIF(dqr.total_products_seen, 0), 2) AS failure_rate_percent
FROM pipeline_runs pr
JOIN data_quality_reports dqr
    ON pr.process_run_id = dqr.process_run_id;