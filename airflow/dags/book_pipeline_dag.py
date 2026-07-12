from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.fahasa_crawler_scaled import crawl_raw_catalog_data
from src.tiki_crawler_scaled import crawl_tiki_all
from src.nha_phuong_nam_crawler_scaled import crawl_phuongnam_all
from src.vinabook_crawler_scaled import crawl_vinabook_all
from src.bookbuy_crawler_scaled import crawl_bookbuy_all

from src.spark_process_all_sources import process_all_sources_spark
from src.update_metadata import update_metastore
from src.validate_trino import validate_trino

@dag(
    dag_id = "book_data_pipeline",
    description="Crawl, process and publish book data to Trino",
    schedule = None,
    start_date=datetime(2026,7,1),
    catchup=False,
    tags=["books","minio","trino"],
)
def book_data_pipeline():
    
    @task(
        task_id="crawl_fahasa",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def run_crawl_fahasa():
        crawl_raw_catalog_data()
    
    @task(
        task_id="crawl_tiki",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def run_crawl_tiki():
        crawl_tiki_all()
    
    @task(
        task_id="crawl_phuongnam",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def run_crawl_phuongnam():
        crawl_phuongnam_all()
    
    @task(
        task_id="crawl_vinabook",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def run_crawl_vinabook():
        crawl_vinabook_all()
    
    @task(
        task_id="crawl_bookbuy",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    def run_crawl_bookbuy():
        crawl_bookbuy_all()
    
    @task(
        task_id="process_all_sources",
        retries=0,
    )
    def run_process_all_sources():
        folder_path = process_all_sources_spark()
        if not folder_path:
            raise ValueError("Không có folder_path trả về từ Spark processing")
        print(f"[*] Spark processing xong, folder_path: {folder_path}")
        return folder_path
    
    @task(
        task_id="update_metastore",
        retries=0,
    )    
    def run_update_metastore(folder_path: str):
        update_metastore(folder_path)
    
    @task(
        task_id="validate_trino",
        retries=0,
    )
    def run_validate_trino():
        validate_trino()
    
    crawl1 = run_crawl_fahasa()
    crawl2 = run_crawl_tiki()
    crawl3 = run_crawl_phuongnam()
    crawl4 = run_crawl_vinabook()
    crawl5 = run_crawl_bookbuy()
    
    process = run_process_all_sources()
    update = run_update_metastore(process)
    validate = run_validate_trino()
    
    [crawl1, crawl2, crawl3, crawl4, crawl5] >> process
    update >> validate
    
book_data_pipeline()
