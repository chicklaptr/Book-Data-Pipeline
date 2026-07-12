import glob
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

from src.utils.minio_config import get_minio_client, upload_to_lake, MINIO_BUCKET

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, DataType, StructType

RAW_DIR = os.getenv("RAW_DIR", "data/raw")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "data/processed")
SOURCES = ["tiki", "phuongnam", "fahasa", "vinabook", "bookbuy"]

def now_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def now_iso() -> str:
    return datetime.now().isoformat()

def read_json_auto(spark: SparkSession, file_path: str) -> DataFrame:
    if file_path.endswith(".jsonl"):
        return spark.read.option("lineSep", "\n").json(file_path)
    return spark.read.option("multiline", "true").json(file_path)

def has_col(df: DataFrame, name: str) -> bool:
    return name in df.columns

def col_or_null(df: DataFrame, name: str):
    return F.col(name) if has_col(df, name) else F.lit(None)

# kiem tra cac truong thong tin co ton tai trong schema
def nested_col_or_null(df: DataFrame, path: str):
    try:
        current = df.schema
        parts = path.split(".")
        for part in parts:
            field = next((f for f in current.fields if f.name == part), None)
            if field is None:
                return F.lit(None)
            current = field.dataType
        return F.col(path)
    except Exception:
        return F.lit(None)

def nested_data_type(df: DataFrame, path: str) -> Optional[DataType]:
    """Return a nested field's Spark type, or None when its path is absent."""
    current: DataType = df.schema
    for part in path.split("."):
        if not isinstance(current, StructType):
            return None
        field = next((f for f in current.fields if f.name == part), None)
        if field is None:
            return None
        current = field.dataType
    return current

def to_long(column):
    # 1. Chuyển sang string, thay thế ký tự lạ (bao gồm cả "true"/"false" của boolean)
    cleaned = F.regexp_replace(
        F.coalesce(column.cast("string"), F.lit("")),
        r"[\.\,đ₫\s]|true|false|null",
        ""
    )
    # 2. Dùng rlike để kiểm tra giá trị hợp lệ trước khi cast —
    #    trả về NULL thay vì văng lỗi SparkNumberFormatException khi giá trị không parse được
    return F.when(cleaned.isNull() | (cleaned == "") | (~cleaned.rlike(r"^\d+$")), F.lit(None).cast("long")).otherwise(cleaned.cast("long"))

def to_double(column):
    # Dùng rlike kiểm tra giá trị hợp lệ trước khi cast
    as_str = F.coalesce(column.cast("string"), F.lit(""))
    return F.when(as_str.isNull() | (as_str == "") | (~as_str.rlike(r"^-?\d+\.?\d*$")), F.lit(None).cast("double")).otherwise(as_str.cast("double"))
# ham chuan hoa ten
def build_product_key(name_col):
    x = F.lower(F.trim(F.coalesce(name_col.cast("string"), F.lit(""))))
    x = F.regexp_replace(x, r"\(.*?\)", " ")
    x = F.regexp_replace(x, r"\[.*?\]", " ")

    remove_phrases = [
        "tái bản", "bìa mềm", "bìa cứng", "phiên bản đặc biệt",
        "ấn bản đặc biệt", "tặng kèm", "kèm bookmark", "kèm quà tặng", "khổ lớn",
    ]
    for phrase in remove_phrases:
        x = F.regexp_replace(x, phrase, " ")

    x = F.regexp_replace(x, r"[^0-9A-Za-zÀ-ỹ\s]", " ")
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    return F.when(x == "", F.lit(None)).otherwise(x)

# them ten da chuan hoa cho truong thong tin cua san pham 
def add_common_fields(df: DataFrame, execution_time) -> DataFrame:
    df = df.withColumn("product_key", build_product_key(F.col("product_name")))
    df = df.withColumn("name_key", F.col("product_key"))
    # Sử dụng timestamp tĩnh của đợt chạy để đồng bộ dữ liệu
    df = df.withColumn("processed_time", F.lit(execution_time))
    return df

# chuan hóa dang dữ liệu cho nguồn tiki 
def normalize_tiki(df: DataFrame, exec_time) -> DataFrame:
    out = df.select(
        F.lit("tiki").alias("source_name"),
        F.coalesce(col_or_null(df, "standard_category"), F.lit("BOOK")).alias("standard_category"),
        col_or_null(df, "source_category_id").cast("string").alias("source_category_id"),
        col_or_null(df, "source_category_name").alias("source_category_name"),
        col_or_null(df, "source_parent_category_name").alias("source_parent_category_name"),
        F.lit(None).cast("string").alias("source_category_url"),
        col_or_null(df, "product_id").cast("string").alias("product_id"),
        col_or_null(df, "product_name").alias("product_name"),
        to_long(col_or_null(df, "price")).alias("price"),
        to_long(col_or_null(df, "original_price")).alias("original_price"),
        to_long(col_or_null(df, "price")).alias("final_price"),
        to_long(col_or_null(df, "discount")).cast("int").alias("discount"),
        to_double(col_or_null(df, "rating_average")).alias("rating_average"),
        to_long(col_or_null(df, "review_count")).alias("review_count"),
        to_long(col_or_null(df, "sold_qty")).alias("sold_qty"),
        F.lit(None).cast("int").alias("stock_available"),
        col_or_null(df, "product_url").cast("string").alias("product_url"),
        col_or_null(df, "image_url").alias("image_url"),
        col_or_null(df, "crawl_page").cast("int").alias("crawl_page"),
        col_or_null(df, "crawl_time").cast("string").alias("crawl_time"),
    )
    return add_common_fields(out, exec_time)

# chuẩn hóa cho các nguồn như bookbuy, phuongnam,vinaphone
def normalize_simple_book_source(df: DataFrame, source_name: str, exec_time) -> DataFrame:
    out = df.select(
        F.lit(source_name).alias("source_name"),
        F.coalesce(col_or_null(df, "standard_category"), F.lit("BOOK")).alias("standard_category"),
        F.lit(None).cast("string").alias("source_category_id"),
        col_or_null(df, "source_category_name").alias("source_category_name"),
        F.lit(None).cast("string").alias("source_parent_category_name"),
        col_or_null(df, "source_category_url").alias("source_category_url"),
        col_or_null(df, "product_id").cast("string").alias("product_id"),
        col_or_null(df, "product_name").alias("product_name"),
        to_long(col_or_null(df, "price")).alias("price"),
        to_long(col_or_null(df, "original_price")).alias("original_price"),
        to_long(col_or_null(df, "price")).alias("final_price"),
        to_long(col_or_null(df, "discount")).cast("int").alias("discount"),
        to_double(col_or_null(df, "rating_average")).alias("rating_average"),
        to_long(col_or_null(df, "review_count")).alias("review_count"),
        to_long(col_or_null(df, "sold_qty")).alias("sold_qty"),
        to_long(col_or_null(df, "stock_available")).cast("int").alias("stock_available"),
        col_or_null(df, "product_url").cast("string").alias("product_url"),
        col_or_null(df, "image_url").alias("image_url"),
        col_or_null(df, "crawl_page").cast("int").alias("crawl_page"),
        col_or_null(df, "crawl_time").cast("string").alias("crawl_time"),
    )
    return add_common_fields(out, exec_time)

# chuẩn hóa dạng dữ liệu cho nguồn fahasa
def normalize_fahasa(df: DataFrame, exec_time) -> DataFrame:
    product_list_type = nested_data_type(df, "body.product_list")
    if not isinstance(product_list_type, ArrayType):
        # An empty/corrupt JSONL file has no schema, so Spark represents the
        # missing path as NULL (VOID). explode_outer(NULL) is an analysis error.
        # Return a typed, empty result so the remaining sources can still run.
        print("[WARN] Fahasa raw file has no array body.product_list; skipping it.")
        out = df.limit(0).select(
            F.lit("fahasa").alias("source_name"),
            F.lit("BOOK").alias("standard_category"),
            F.lit(None).cast("string").alias("source_category_id"),
            F.lit(None).cast("string").alias("source_category_name"),
            F.lit(None).cast("string").alias("source_parent_category_name"),
            F.lit(None).cast("string").alias("source_category_url"),
            F.lit(None).cast("string").alias("product_id"),
            F.lit(None).cast("string").alias("product_name"),
            F.lit(None).cast("long").alias("price"),
            F.lit(None).cast("long").alias("original_price"),
            F.lit(None).cast("long").alias("final_price"),
            F.lit(None).cast("int").alias("discount"),
            F.lit(None).cast("double").alias("rating_average"),
            F.lit(None).cast("long").alias("review_count"),
            F.lit(None).cast("long").alias("sold_qty"),
            F.lit(None).cast("int").alias("stock_available"),
            F.lit(None).cast("string").alias("product_url"),
            F.lit(None).cast("string").alias("image_url"),
            F.lit(None).cast("int").alias("crawl_page"),
            F.lit(None).cast("string").alias("crawl_time"),
        )
        return add_common_fields(out, exec_time)

    exploded = df.withColumn("p", F.explode_outer(F.col("body.product_list")))
    out = exploded.select(
        F.lit("fahasa").alias("source_name"),
        F.lit("BOOK").alias("standard_category"),
        F.lit(None).cast("string").alias("source_category_id"),
        col_or_null(exploded, "source_category_name").alias("source_category_name"),
        F.lit(None).cast("string").alias("source_parent_category_name"),
        col_or_null(exploded, "source_category_url").alias("source_category_url"),
        nested_col_or_null(exploded, "p.product_id").cast("string").alias("product_id"),
        nested_col_or_null(exploded, "p.product_name").alias("product_name"),
        to_long(nested_col_or_null(exploded, "p.product_price")).alias("price"),
        to_long(nested_col_or_null(exploded, "p.product_price")).alias("original_price"),
        to_long(nested_col_or_null(exploded, "p.product_finalprice")).alias("final_price"),
        to_long(nested_col_or_null(exploded, "p.discount")).cast("int").alias("discount"),
        F.lit(None).cast("double").alias("rating_average"),
        F.lit(None).cast("long").alias("review_count"),
        to_long(nested_col_or_null(exploded, "p.sold_qty")).alias("sold_qty"),
        to_long(nested_col_or_null(exploded, "p.stock_available")).cast("int").alias("stock_available"),
        nested_col_or_null(exploded, "p.product_url").cast("string").alias("product_url"),
        nested_col_or_null(exploded, "p.image_src").alias("image_url"),
        F.lit(None).cast("int").alias("crawl_page"),
        col_or_null(exploded, "crawl_time").cast("string").alias("crawl_time"),
    )
    return add_common_fields(out, exec_time)

# chuẩn hóa cho từng nguồn
def normalize_source(spark: SparkSession, source: str, raw_file: str, exec_time) -> DataFrame:
    raw_df = read_json_auto(spark, raw_file)
    if source == "tiki": return normalize_tiki(raw_df, exec_time)
    if source == "phuongnam": return normalize_simple_book_source(raw_df, "phuongnam", exec_time)
    if source == "vinabook": return normalize_simple_book_source(raw_df, "vinabook", exec_time)
    if source == "bookbuy": return normalize_simple_book_source(raw_df, "bookbuy", exec_time)
    if source == "fahasa": return normalize_fahasa(raw_df, exec_time)
    raise ValueError(f"Nguồn chưa hỗ trợ: {source}")

# xác minh các lỗi có thể xả ra
def add_validation_errors(df: DataFrame) -> DataFrame:
    errors = F.array_remove(
        F.array(
            F.when(F.col("product_name").isNull() | (F.trim(F.col("product_name")) == ""), F.lit("missing_product_name")),
            F.when(F.col("price").isNotNull() & (F.col("price") < 0), F.lit("invalid_price_negative")),
            F.when(F.col("original_price").isNotNull() & (F.col("original_price") < 0), F.lit("invalid_original_price_negative")),
        ),
        None,
    )
    return df.withColumn("validation_errors", errors)

# lưu tóm tắt dữ liệu vào file
def write_single_json(summary: list | dict, file_path: str) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

def process_all_sources_spark() -> str:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    timestamp = now_str()
    exec_time_fixed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # tạo phiên spark để xử lý dữ liệu
    spark = (
        SparkSession.builder
        .appName("BookMultiSourceBigDataProcessing")
        .master("local[*]")  # Chạy local và dùng TẤT CẢ các nhân CPU có sẵn
        .config("spark.driver.memory", "4g")      #  Cấp 4GB RAM cho Driver (tránh OOM khi .collect() hoặc .take())
        .config("spark.executor.memory", "4g")    # Cấp 4GB RAM cho Executor xử lý dữ liệu
        .config("spark.sql.shuffle.partitions", "8") # Giữ nguyên vì bạn chạy ít nguồn dữ liệu
        .config("spark.memory.offHeap.enabled", "true") # Bật bộ nhớ ngoài Heap để tối ưu hóa bộ nhớ cho JVM
        .config("spark.memory.offHeap.size", "2g")
        .getOrCreate()
    )
    
    client = get_minio_client()
    source_files: Dict[str, str] = {}
    source_counts_before_validate: Dict[str, int] = {}
    normalized_dfs: List[DataFrame] = []

    try:
        for source in SOURCES:
            prefix = f"raw/{source}/"
            objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
            valid_files = [o for o in objects if "summary" not in o.object_name.lower() and "stats" not in o.object_name.lower()]
            
            if not valid_files: continue
            
            latest_obj = max(valid_files, key=lambda x: x.last_modified)
            local_raw_path = os.path.join(RAW_DIR, os.path.basename(latest_obj.object_name))
            
            client.fget_object(MINIO_BUCKET, latest_obj.object_name, local_raw_path)

            print(f"\n===== SPARK READ {source}: {local_raw_path} =====")
            source_files[source] = local_raw_path
            normalized = normalize_source(spark, source, local_raw_path, exec_time_fixed)
            normalized = normalized.withColumn("raw_file", F.lit(local_raw_path))

            # Cache lại vì debug cần dùng count() và sau đó còn gộp
            normalized.cache()
            source_counts_before_validate[source] = normalized.count()
            normalized_dfs.append(normalized)

        if not normalized_dfs:
            raise RuntimeError("Không có dữ liệu raw nào để xử lý.")

        all_products = normalized_dfs[0]
        for df in normalized_dfs[1:]:
            all_products = all_products.unionByName(df, allowMissingColumns=True)
        
        # --- CẤU HÌNH CỬA SỔ ĐỂ XỬ LÝ TRÙNG NỘI BỘ ---
        # Giả sử bạn muốn lấy dòng mới nhất (dựa trên crawl_time)
        # Phuong Nam does not provide product_id on collection pages.  Use the
        # product URL (then the normalized title) as a stable fallback so all
        # NULL IDs are not collapsed into one record by the window function.
        all_products = all_products.withColumn(
            "_dedup_key",
            F.when(
                F.col("product_id").isNotNull() & (F.trim(F.col("product_id")) != ""),
                F.col("product_id").cast("string"),
            ).otherwise(F.coalesce(F.col("product_url"), F.col("product_key"))),
        )

        window_spec = Window.partitionBy("source_name", "_dedup_key") \
                            .orderBy(F.col("crawl_time").desc())

        # Đánh số thứ tự cho các dòng trùng ID trong cùng nguồn
        all_products = all_products.withColumn("row_num", F.row_number().over(window_spec))

        # Chỉ giữ lại dòng có row_num = 1 (tức là dòng crawl_time mới nhất)
        all_products = all_products.filter(F.col("row_num") == 1).drop("row_num", "_dedup_key")

        checked = add_validation_errors(all_products)
        
        # Phân tách tập dữ liệu & Cache để tránh re-evaluation nhiều lần
        valid = checked.filter(F.col("validation_errors").isNull() | (F.size(F.col("validation_errors")) == 0)).drop("validation_errors").cache()
        bad = checked.filter(F.col("validation_errors").isNotNull() & (F.size(F.col("validation_errors")) > 0)).cache()

        # Tạo dict an toàn cho counts sau validate (tránh rỗng khóa)
        source_counts_after_validate = {s: 0 for s in source_counts_before_validate.keys()}
        for row in valid.groupBy("source_name").count().collect():
            source_counts_after_validate[row["source_name"]] = row["count"]


        #Chuẩn hóa tên , và từ tên chuẩn hóa lấy ra 3 kí tự để làm nhóm block chung để so sanh giảm phép tính cần tính 
        df_with_block = valid.withColumn("cleaned_name",build_product_key(F.col("product_name")))\
                            .withColumn("block_key",F.substring(F.col("cleaned_name"), 1, 6))\
                                .cache()
        
        # hàm tính toán tổng độ đo tương đồng của hai phần tử(bị thay thế cho phù hợp với spark để tránh bị tràn ram)
        #chuyển thành mảng các từ cho vào cột mới
        df_with_array = df_with_block.withColumn("word_array",F.split(F.col("cleaned_name"), " "))
        
        #Thực hiện phép join bắt cặp giữa các nguồn sách
        pairs = df_with_array.alias("a").join(
                df_with_array.alias("b"),
                (F.col("a.block_key") == F.col("b.block_key")) & # Giữ block_key để giới hạn không gian join
                (F.col("a.source_name") < F.col("b.source_name")) & # Chỉ so sánh chéo nguồn
                # --- ĐIỀU KIỆN CHẶT HƠN ---
                (F.abs(F.col("a.price") - F.col("b.price")) / F.greatest(F.col("a.price"), F.col("b.price")) < 0.3) & # Giá chênh lệch dưới 30%
                (F.col("a.product_key") != F.col("b.product_key")) # Tên sau khi chuẩn hóa phải khác nhau (vì nếu giống nhau thì đã là 1)
            )
        #Tính jaccard Score  bằng hàm spark thuần 
        scored_pairs = pairs.withColumn("intersect_size",F.size(F.array_intersect(F.col("a.word_array"),F.col("b.word_array"))))\
                            .withColumn("union_size",F.size(F.array_distinct(F.array_union(F.col("a.word_array"),F.col("b.word_array")))))\
                            .withColumn("name_score",F.when(F.col("union_size") > 0, F.col("intersect_size")/F.col("union_size")).otherwise(0.0))
                        
        #Tinhs tỉ lệ tương đồng của hai đối tượng xem khả năng trùng lặp
        scored_pairs = scored_pairs.withColumn(
            "similarity_score",
            F.col("name_score") *0.8 + F.when(
                (F.col("a.price").isNotNull()) & (F.col("b.price").isNotNull()) & (F.col("a.price") > 0) &
                (F.abs(F.col("a.price")-F.col("b.price")) / F.col("a.price") < 0.3), 
                F.lit(0.2) 
            ).otherwise(F.lit(0.1))
        )
        
        #Lọc các cặp trùng lặp dựa trên ngưỡng điểm số
        matched_pairs = scored_pairs.filter(F.col("similarity_score") >= 0.85).cache()
                        
        duplicate_ids_df = matched_pairs.select(F.col("b.product_id").alias("dup_id"),F.col("b.source_name").alias("dup_source")).distinct().cache()
        
        duplicate = df_with_array.join(
            F.broadcast(duplicate_ids_df),
            (df_with_array.product_id == duplicate_ids_df.dup_id) &
            (df_with_array.source_name == duplicate_ids_df.dup_source),
            "inner"
        ).drop("cleaned_name","block_key","word_array","dup_id","dup_source").cache()
        
        dedup = df_with_array.join(
            F.broadcast(duplicate_ids_df),
            (df_with_array.product_id == duplicate_ids_df.dup_id)&
            (df_with_array.source_name == duplicate_ids_df.dup_source),
            "left_anti"
        ).drop("cleaned_name","block_key","word_array").cache()
                    
        base_output_dir = os.path.join(PROCESSED_DIR, f"spark_run_{timestamp}")
        
        output_parquet_single = os.path.join(base_output_dir, "all_books_processed_single_parquet")
        output_parquet_partitioned = os.path.join(base_output_dir, "all_books_processed_partitioned_parquet")

        bad_json_file = os.path.join(base_output_dir, "bad_products.json")
        duplicate_json_file = os.path.join(base_output_dir, "duplicate_products.json")
        summary_file = os.path.join(base_output_dir, "spark_processing_summary.json")

        # Ghi dữ liệu chính
        dedup.coalesce(1).write.mode("overwrite").parquet(output_parquet_single)
        dedup.repartition("source_name").write.mode("overwrite").partitionBy("source_name").parquet(output_parquet_partitioned)
        
        # Thu thập log có GIỚI HẠN (.take()) phòng chống OOM nổ Driver RAM
        bad_count = bad.count()
        if bad_count > 0:
            bad_list = [row.asDict() for row in bad.select("source_name","product_id","product_name","price","validation_errors").take(2000)] # Giới hạn tối đa 2000 dòng log lỗi
            write_single_json(bad_list, bad_json_file)
        else:
            write_single_json([], bad_json_file)

        duplicate_count = duplicate.count()
        if duplicate_count > 0:
            dup_list = [row.asDict() for row in duplicate.select("source_name","product_id","product_name","price","processed_time").take(2000)] # Giới hạn tối đa 2000 dòng trùng lặp
            for r in dup_list:
                if r.get("processed_time"): r["processed_time"] = str(r["processed_time"])
            write_single_json(dup_list, duplicate_json_file)
        else:
            write_single_json([], duplicate_json_file)

        source_counts_after_dedup = {s: 0 for s in source_counts_before_validate.keys()}
        for row in dedup.groupBy("source_name").count().collect():
            source_counts_after_dedup[row["source_name"]] = row["count"]

        summary = {
            "processed_at": now_iso(),
            "engine": "pyspark",
            "source_files": source_files,
            "source_counts_before_validate": source_counts_before_validate,
            "source_counts_after_validate": source_counts_after_validate,
            "source_counts_after_dedup": source_counts_after_dedup,
            "total_valid_before_dedup": valid.count(),
            "total_after_dedup": dedup.count(),
            "bad_products": bad_count,
            "duplicate_products": duplicate_count,
            "output_parquet_single": output_parquet_single,
            "output_parquet_partitioned": output_parquet_partitioned,
            "bad_output": bad_json_file,
            "duplicate_output": duplicate_json_file,
            "summary_file": summary_file,
        }
        write_single_json(summary, summary_file)

        print("\n[*] Đang đồng bộ kết quả Spark lên MinIO Data Lake...")
        processed_prefix = f"processed/spark_run_{timestamp}"
        for root, _, files in os.walk(base_output_dir):
            for file in files:
                if file.startswith(".") or file == "_SUCCESS": continue
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, base_output_dir).replace("\\", "/")
                minio_object_name = f"{processed_prefix}/{relative_path}"
                upload_to_lake(file_path=local_file_path, object_name=minio_object_name)

        print("\n===== SPARK PROCESSING DONE & SYNCED TO MINIO =====")
        return f"s3://{MINIO_BUCKET}/{processed_prefix}/all_books_processed_single_parquet/"

    finally:
        print("\n[*] Đang giải phóng bộ nhớ và dừng Spark Session...")
        # 1. Giải phóng danh sách dataframe raw
        if 'normalized_dfs' in locals():
            for df in normalized_dfs:
                try: df.unpersist()
                except: pass
                
        # 2. Giải phóng các dataframe trung gian đã cache trong quá trình Join Jaccard
        cache_dfs = [
            "valid", "bad", "df_with_block", "df_with_array", 
            "matched_pairs", "duplicate_ids_df", "duplicate", "dedup"
        ]
        for df_name in cache_dfs:
            if df_name in locals() and locals()[df_name] is not None:
                try:
                    locals()[df_name].unpersist()
                except:
                    pass
                    
        # 3. Dừng hẳn Spark
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    process_all_sources_spark()
