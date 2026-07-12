import os
import logging
from minio import Minio

# Thiết lập log cho MinIO
logger = logging.getLogger("MinIO-Manager")

# SỬA Ở ĐÂY: Thay 'minio:9000' bằng 'localhost:9000' để an toàn cho việc chạy trên máy Windows
# Khi chạy trong Docker, bạn chỉ cần set biến môi trường MINIO_ENDPOINT=minio:9000 là nó sẽ tự ghi đè.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000") 

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "data-lake")

def get_minio_client() -> Minio:
    """Khởi tạo và trả về MinIO Client"""
    try:
        # Thêm log để biết đang kết nối tới đâu (Rất quan trọng khi debug)
        logger.info(f"Đang kết nối MinIO tại: {MINIO_ENDPOINT}")
        
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False # MinIO local thường không dùng HTTPS
        )
        return client
    except Exception as e:
        logger.error(f"Không thể khởi tạo MinIO Client: {e}")
        raise


def ensure_bucket_exists(client: Minio, bucket_name: str = MINIO_BUCKET) -> None:
    """Kiểm tra và tự động tạo bucket nếu chưa có"""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Đã tạo mới bucket thành công: '{bucket_name}'")
        else:
            logger.debug(f"Bucket '{bucket_name}' đã tồn tại.")
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra/tạo Bucket: {e}")
        raise


def upload_to_lake(file_path: str, object_name: str, bucket_name: str = MINIO_BUCKET) -> str:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file cục bộ để upload: {file_path}")
        
    client = get_minio_client()
    ensure_bucket_exists(client, bucket_name)
    
    try:
        client.fput_object(bucket_name, object_name, file_path)
        logger.info(f" Đã đẩy file thành công lên MinIO -> [{bucket_name}]: {object_name}")
        return object_name
    except Exception as e:
        logger.error(f"Thất bại khi đẩy file {file_path} lên MinIO: {e}")
        raise