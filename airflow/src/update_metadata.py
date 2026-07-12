import os
import trino

TABLE_NAME = "hive.default.books_final"

def update_metastore(folder_path: str) -> None:
    # 1. Cấu hình kết nối
    conn = trino.dbapi.connect(
       host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "admin"),
        catalog="hive",
        schema="default",
    )
    cur = conn.cursor()

    # 2. Đường dẫn thư mục chứa file Parquet (đã xác định ở bước trước)
    # Lưu ý: Chỉ trỏ vào thư mục cha (folder), không trỏ vào tên file lẻ
    
    if not folder_path:
        raise ValueError("folder_path đang rỗng")
    trino_folder_path = folder_path.replace("s3a://", "s3://", 1)

    # 3. Câu lệnh tạo bảng với 24 cột chuẩn
    create_query = f"""
    CREATE TABLE {TABLE_NAME} (
        source_name varchar,
        standard_category varchar,
        source_category_id varchar,
        source_category_name varchar,
        source_parent_category_name varchar,
        source_category_url varchar,
        product_id varchar,
        product_name varchar,
        price bigint,
        original_price bigint,
        final_price bigint,
        discount integer,
        rating_average double,
        review_count bigint,
        sold_qty bigint,
        stock_available integer,
        product_url varchar,
        image_url varchar,
        crawl_page integer,
        crawl_time varchar,
        product_key varchar,
        name_key varchar,
        processed_time varchar,
        raw_file varchar
    ) 
    WITH (
        format = 'PARQUET',
        external_location = '{trino_folder_path}'
    )
    """ 

    try:
        print(f"Đang xóa bảng cũ: {TABLE_NAME}")

        cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        cur.fetchall()

        print(f"Đang tạo bảng mới với đường dẫn: {trino_folder_path}")

        cur.execute(create_query)
        cur.fetchall()

        print("Đã tạo bảng thành công")

        # Kiểm tra cấu trúc bảng
        cur.execute(f"DESCRIBE {TABLE_NAME}")

        print("\n--- Cấu trúc bảng ---")
        for row in cur.fetchall():
            print(f"Cột: {row[0]:<35} | Kiểu: {row[1]}")

        
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()