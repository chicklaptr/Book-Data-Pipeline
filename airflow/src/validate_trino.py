import os
import trino


TABLE_NAME = "hive.default.books_final"
REQUIRED_SOURCES = {"tiki", "phuongnam", "vinabook", "bookbuy"}
# Fahasa is temporarily optional because its Cloudflare challenge prevents the
# crawler from collecting data. It remains validated whenever it is present.
OPTIONAL_SOURCES = {"fahasa"}
EXPECTED_SOURCE_COUNT = "4 or 5"

def validate_trino() -> None:
    """
    Kiểm tra luồng dữ liệu cuối:

    MinIO Parquet
        -> Hive Metastore
        -> Trino
        -> kết quả truy vấn

    Hàm chạy hết không lỗi: task SUCCESS.
    Hàm raise exception: task FAILED.
    """
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "admin"),
        catalog="hive",
        schema="default",
    )
    
    cur = conn.cursor()
    
    try:
        print(f"kiểm tra bảng {TABLE_NAME}")
        
        # check schema
        cur.execute(
            f"""
                SELECT *
                FROM {TABLE_NAME}
                LIMIT 5
            """
        )
        
        sample_rows = cur.fetchall()

        if not sample_rows:
            raise ValueError(f"Không có dữ liệu trong bảng {TABLE_NAME}")
        
        print(f"Đọc thành công {len(sample_rows)} dòng mẫu từ {TABLE_NAME}")
        
        
        # tong quan dataset
        cur.execute(
            f"""
                SELECT COUNT(*) as total_rows,
                        MAX(TRY_CAST(crawl_time AS TIMESTAMP)) as last_crawl_time
                FROM {TABLE_NAME}
            """
        )
        
        result = cur.fetchone()
        
        if result is None:
            raise ValueError(f"Không thể truy vấn tổng quan dữ liệu từ {TABLE_NAME}")
        total_rows, last_crawl_time = result

        cur.execute(f"SELECT DISTINCT source_name FROM {TABLE_NAME}")
        source_names = {row[0] for row in cur.fetchall() if row[0]}
        missing_required = REQUIRED_SOURCES - source_names
        unexpected_sources = source_names - REQUIRED_SOURCES - OPTIONAL_SOURCES
        count_sources = len(source_names)

        if missing_required:
            raise ValueError(f"Missing required sources: {sorted(missing_required)}")
        if unexpected_sources:
            raise ValueError(f"Unsupported sources: {sorted(unexpected_sources)}")

        missing_optional = OPTIONAL_SOURCES - source_names
        if missing_optional:
            print(f"Warning: optional sources unavailable: {sorted(missing_optional)}")
        
        print(f"Tổng số dòng: {total_rows}, số nguồn: {count_sources}, crawl_time mới nhất: {last_crawl_time}")
        
        if total_rows <= 0:
            raise ValueError(f"Khong co du lieu trong bang {TABLE_NAME}")
        
        if count_sources not in (4, 5):
            raise ValueError(f"Số nguồn dữ liệu không đúng: {count_sources} != {EXPECTED_SOURCE_COUNT}")
        
        if last_crawl_time is None:
            raise ValueError(f"Crawl time mới nhất không hợp lệ: {last_crawl_time}")
        
        print(f"Kiểm tra bảng {TABLE_NAME} thành công!")
        
    except Exception as e:
        print(f"Lỗi khi kiểm tra bảng {TABLE_NAME}: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()
        
if __name__ == "__main__":
    validate_trino()
        
        
