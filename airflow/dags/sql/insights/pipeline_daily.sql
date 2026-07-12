CREATE OR REPLACE VIEW vw_dm_pipeline_health_daily AS
WITH rejected_daily AS (
    SELECT
        DATE(rejected_at) AS run_date,
        COUNT(*) AS total_rejected
    FROM rejected_products
    GROUP BY DATE(rejected_at)
),
audit_daily AS (
    SELECT
        DATE(finished_at) AS run_date,
        COUNT(*) AS total_tasks,
        COUNT(*) FILTER (WHERE status = 'success') AS success_tasks,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed_tasks
    FROM pipeline_task_audit
    GROUP BY DATE(finished_at)
)
SELECT 
    DATE(pr.processed_at) AS run_date,
    COUNT(DISTINCT pr.process_run_id) AS total_runs,
    SUM(COALESCE(dqr.total_products_seen, 0)) AS total_products_seen,
    SUM(COALESCE(dqr.products_passed, 0)) AS total_products_passed,
    SUM(COALESCE(dqr.products_failed, 0)) AS total_products_failed,
    SUM(COALESCE(dqr.duplicate_product_id, 0)) AS total_duplicate_product_id,
    ROUND(
        CASE
            WHEN SUM(COALESCE(dqr.total_products_seen, 0)) = 0 THEN 0
            ELSE
                SUM(COALESCE(dqr.products_failed, 0))::numeric
                / SUM(COALESCE(dqr.total_products_seen, 0))::numeric * 100
        END,
        2
    ) AS failure_rate_percent,
    COALESCE(rd.total_rejected, 0) AS total_rejected,
    COALESCE(ad.total_tasks, 0) AS total_tasks,
    COALESCE(ad.success_tasks, 0) AS success_tasks,
    COALESCE(ad.failed_tasks, 0) AS failed_tasks
FROM pipeline_runs pr
LEFT JOIN data_quality_reports dqr
    ON pr.process_run_id = dqr.process_run_id
LEFT JOIN rejected_daily rd
    ON DATE(pr.processed_at) = rd.run_date
LEFT JOIN audit_daily ad
    ON DATE(pr.processed_at) = ad.run_date
GROUP BY 
    DATE(pr.processed_at),
    rd.total_rejected,
    ad.total_tasks,
    ad.success_tasks,
    ad.failed_tasks;