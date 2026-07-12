import json
import os
import time
import requests
from datetime import datetime
from src.utils.minio_config import upload_to_lake

SOURCE_NAME = "tiki"

CATEGORY_API = "https://tiki.vn/api/v2/categories"
LISTING_API = "https://tiki.vn/api/personalish/v1/blocks/listings"

# Tăng page bằng env: MAX_PAGE_PER_CATEGORY=50 python tiki_crawler_scaled.py
MAX_PAGE_PER_CATEGORY = int(os.getenv("MAX_PAGE_PER_CATEGORY", "200"))
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.7"))
LIMIT_PER_PAGE = int(os.getenv("LIMIT_PER_PAGE", "40"))
MAX_CONSECUTIVE_PAGE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_PAGE_ERRORS", "3"))

# False = lấy toàn bộ category con tìm được từ API thay vì chỉ 9 category cũ.
FILTER_TARGET_CATEGORIES = os.getenv("FILTER_TARGET_CATEGORIES", "false").lower() == "true"

CATEGORY_CONFIG = {
    "VAN_HOC": [
        "Tiểu Thuyết",
        "Truyện ngắn - Tản văn - Tạp văn",
        "Truyện trinh thám",
    ],
    "KINH_TE": [
        "Sách quản trị, lãnh đạo",
        "Marketing - Bán hàng",
        "Sách khởi nghiệp",
    ],
    "KY_NANG": [
        "Sách tư duy - Kỹ năng sống",
        "Sách tâm lý học",
        "Sách kỹ năng mềm",
    ],
}

# Mở rộng parent category sách. Nếu ID nào không còn hợp lệ, code sẽ bỏ qua và log lỗi.
PARENT_ID = {
    "Sách văn học": 839,
    "Sách kinh tế": 846,
    "Sách kỹ năng sống": 870,
    "Sách thiếu nhi": 832,
    "Sách giáo khoa - tham khảo": 893,
    "Sách ngoại ngữ": 871,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}


def now_iso():
    return datetime.now().isoformat()

def map_tiki_standard_category(parent_name, category_name):
    text = f"{parent_name or ''} {category_name or ''}".lower()

    if any(k in text for k in ["sách", "văn học", "kinh tế", "ngoại ngữ", "giáo khoa"]):
        return "BOOK"
    if any(k in text for k in ["điện thoại", "máy tính bảng", "laptop", "thiết bị số", "linh kiện"]):
        return "ELECTRONICS"
    if any(k in text for k in ["điện gia dụng", "nhà cửa", "đời sống", "nội thất"]):
        return "HOME_LIVING"
    if any(k in text for k in ["làm đẹp", "sức khỏe", "mỹ phẩm", "chăm sóc"]):
        return "BEAUTY_HEALTH"
    if any(k in text for k in ["mẹ", "bé", "đồ chơi"]):
        return "MOM_BABY"
    if any(k in text for k in ["thời trang", "giày", "dép", "túi", "đồng hồ", "trang sức"]):
        return "FASHION"
    if any(k in text for k in ["bách hóa", "thực phẩm", "đồ uống"]):
        return "GROCERY"
    if any(k in text for k in ["thể thao", "dã ngoại"]):
        return "SPORT"
    if any(k in text for k in ["ô tô", "xe máy", "xe đạp"]):
        return "AUTO"

    return "OTHER"

def save_jsonl_row(file_path, row):
    """Ghi từng dòng để chạy lâu không bị mất data nếu crash giữa đêm."""
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        
def save_json(data, file_path):
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# các categories con 
def get_tiki_categories(parent_id):
    params = {"include": "children", "parent_id": parent_id}
    res = requests.get(CATEGORY_API, params=params, headers=HEADERS, timeout=20)
    res.raise_for_status()
    data = res.json()
    categories = data.get("data", [])

    result = []

    def walk(nodes, parent_name=None):
        for c in nodes or []:
            result.append({
                "category_id": c.get("id"),
                "category_name": c.get("name"),
                "url_key": c.get("url_key"),
                "parent_name": parent_name,
            })
            children = c.get("children") or []
            if children:
                walk(children, c.get("name"))

    walk(categories)
    return [c for c in result if c.get("category_id") and c.get("url_key")]


def crawl_tiki_category(category, max_page=MAX_PAGE_PER_CATEGORY, output_file=None):
    products = []
    category_id = category["category_id"]
    category_name = category["category_name"]
    url_key = category["url_key"]

    consecutive_errors = 0
    for page in range(1, max_page + 1):
        params = {
            "limit": LIMIT_PER_PAGE,
            "include": "advertisement",
            "aggregations": 2,
            "version": "home-persionalized",
            "category": category_id,
            "page": page,
            "urlKey": url_key,
        }

        try:
            res = requests.get(LISTING_API, params=params, headers=HEADERS, timeout=20)
            if res.status_code != 200 or "json" not in res.headers.get("content-type", "").lower():
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_PAGE_ERRORS:
                    print(f"Stopping {category_name}: {consecutive_errors} consecutive failed responses")
                    break
                print(f"Lỗi {category_name} page {page}: {res.status_code}")
                continue

            data = res.json()
            items = data.get("data", [])
            if not items:
                print(f"Hết data: {category_name} page {page}")
                break

            for p in items:
                product = {
                    "standard_category": map_tiki_standard_category(category.get("parent_name"), category_name),
                    "source_name": SOURCE_NAME,
                    "source_category_id": category_id,
                    "source_category_name": category_name,
                    "source_parent_category_name": category.get("parent_name"),
                    "product_id": p.get("id"),
                    "product_name": p.get("name"),
                    "price": p.get("price"),
                    "original_price": p.get("original_price"),
                    "discount": p.get("discount"),
                    "rating_average": p.get("rating_average"),
                    "review_count": p.get("review_count"),
                    "sold_qty": (
                        p.get("quantity_sold", {}).get("value")
                        if isinstance(p.get("quantity_sold"), dict)
                        else None
                    ),
                    "product_url": (
                        "https://tiki.vn/" + p.get("url_path")
                        if p.get("url_path") else None
                    ),
                    "image_url": p.get("thumbnail_url"),
                    "crawl_page": page,
                    "crawl_time": now_iso(),
                    "raw": p,
                }
                products.append(product)
                if output_file:
                    save_jsonl_row(output_file, product)

            print(f"{category_name} | page {page} | lấy {len(items)} sản phẩm")
            time.sleep(REQUEST_SLEEP_SECONDS)

        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_PAGE_ERRORS:
                print(f"Stopping {category_name}: {consecutive_errors} consecutive errors")
                break
            print(f"Lỗi crawl {category_name} page {page}: {e}")
            time.sleep(REQUEST_SLEEP_SECONDS)

    return products


def crawl_tiki_all(max_page_per_category=MAX_PAGE_PER_CATEGORY, output_file=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if output_file is None:
        output_file = f"data/raw/tiki_raw_{timestamp}.jsonl"

    all_products = []
    target_names = [name for group in CATEGORY_CONFIG.values() for name in group]
    seen_category_ids = set()

    for parent_name, parent_id in PARENT_ID.items():
        try:
            categories = get_tiki_categories(parent_id=parent_id)
        except Exception as e:
            print(f"Lỗi lấy category parent={parent_name} id={parent_id}: {e}")
            continue

        if FILTER_TARGET_CATEGORIES:
            selected_categories = [c for c in categories if c["category_name"] in target_names]
        else:
            selected_categories = categories

        # chống trùng category nếu parent API trả overlap
        selected_categories = [
            c for c in selected_categories
            if c["category_id"] not in seen_category_ids and not seen_category_ids.add(c["category_id"])
        ]

        print(f"\nParent: {parent_name} | số category sẽ crawl: {len(selected_categories)}")
        for c in selected_categories:
            print(" -", c)

        for category in selected_categories:
            products = crawl_tiki_category(
                category=category,
                max_page=max_page_per_category,
                output_file=output_file,
            )
            all_products.extend(products)

    summary_file = f"data/raw/tiki_summary_{timestamp}.json"

    summary = {
        "source_name": SOURCE_NAME,
        "run_id": timestamp,
        "total_products": len(all_products),
        "raw_jsonl_file": output_file,
        "generated_at": now_iso(),
    }

    save_json(summary, summary_file)

    print("Đã lưu JSONL:", output_file)
    print("Đã lưu summary:", summary_file)
    
    # Upload lên MinIO
    try:
        jsonl_obj = f"raw/{SOURCE_NAME}/tiki_raw_{timestamp}.jsonl"
        summary_obj = f"raw/{SOURCE_NAME}/tiki_summary_{timestamp}.json"
        upload_to_lake(output_file, jsonl_obj)
        upload_to_lake(summary_file, summary_obj)
        print(f" Đã upload JSONL: {jsonl_obj}")
        print(f" Đã upload summary: {summary_obj}")
    except Exception as e:
        print(f" Lỗi upload MinIO: {e}")
    
    return all_products

if __name__ == "__main__":
    products = crawl_tiki_all(
        max_page_per_category=MAX_PAGE_PER_CATEGORY,
    )
    print("Tổng sản phẩm:", len(products))
