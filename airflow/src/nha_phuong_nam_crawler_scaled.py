import json
import os
import time
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from src.utils.minio_config import upload_to_lake

SOURCE_NAME = "phuongnam"
MAX_PAGE_PER_CATEGORY = int(os.getenv("MAX_PAGE_PER_CATEGORY", "80"))
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.8"))

# Tăng category tại đây. Các URL dạng /collections/... hỗ trợ ?page=N.
CATEGORY_CONFIG = [
    {"standard_category": "VAN_HOC", "source_category_name": "Văn học", "url": "https://nhasachphuongnam.com/collections/van-hoc"},
    {"standard_category": "TRUYEN_TRANH_MANGA", "source_category_name": "Truyện tranh - Manga", "url": "https://nhasachphuongnam.com/collections/truyen-tranh-manga"},
    {"standard_category": "THIEU_NHI", "source_category_name": "Thiếu nhi", "url": "https://nhasachphuongnam.com/collections/thieu-nhi"},
    {"standard_category": "BAO_TAP_CHI", "source_category_name": "Báo & Tạp chí", "url": "https://nhasachphuongnam.com/collections/bao-tap-chi"},
    {"standard_category": "AM_NHAC_MY_THUAT_THOI_TRANG", "source_category_name": "Âm nhạc - Mỹ thuật - Thời trang", "url": "https://nhasachphuongnam.com/collections/am-nhac-my-thuat-thoi-trang"},
    {"standard_category": "VAN_HOA_NGHE_THUAT_DU_LICH", "source_category_name": "Văn hóa - Nghệ thuật - Du lịch", "url": "https://nhasachphuongnam.com/collections/van-hoa-nghe-thuat-du-lich"},
    {"standard_category": "LICH_SU_DIA_LY", "source_category_name": "Lịch sử - Địa lý", "url": "https://nhasachphuongnam.com/collections/lich-su-dia-ly"},
    {"standard_category": "TON_GIAO_TRIET_HOC", "source_category_name": "Tôn giáo - Triết học", "url": "https://nhasachphuongnam.com/collections/ton-giao-triet-hoc"},
    {"standard_category": "PHONG_THUY_KINH_DICH", "source_category_name": "Phong thủy & Kinh dịch", "url": "https://nhasachphuongnam.com/collections/phong-thuy-kinh-dich"},
    {"standard_category": "KY_NANG_SONG", "source_category_name": "Kỹ năng sống - Sống đẹp", "url": "https://nhasachphuongnam.com/collections/ky-nang-song-song-dep"},
    {"standard_category": "NUOI_DAY_CON", "source_category_name": "Nuôi dạy con", "url": "https://nhasachphuongnam.com/collections/nuoi-day-con"},
    {"standard_category": "NU_CONG_GIA_CHANH", "source_category_name": "Nữ công & Gia chánh", "url": "https://nhasachphuongnam.com/collections/nu-cong-gia-chanh"},
    {"standard_category": "NHA_CUA_LAM_VUON", "source_category_name": "Nhà cửa & Làm vườn", "url": "https://nhasachphuongnam.com/collections/nha-cua-lam-vuon"},
    {"standard_category": "TU_DIEN", "source_category_name": "Từ điển", "url": "https://nhasachphuongnam.com/collections/tu-dien"},
    {"standard_category": "GIAO_TRINH", "source_category_name": "Giáo trình", "url": "https://nhasachphuongnam.com/collections/giao-trinh"},
    {"standard_category": "KHOA_HOC_KY_THUAT", "source_category_name": "Khoa học & Kỹ thuật", "url": "https://nhasachphuongnam.com/collections/khoa-hoc-ky-thuat"},
    {"standard_category": "CHINH_TRI_PHAP_LUAT", "source_category_name": "Chính trị - Pháp luật", "url": "https://nhasachphuongnam.com/collections/chinh-tri-phap-luat"},
    {"standard_category": "KINH_TE", "source_category_name": "Kinh tế", "url": "https://nhasachphuongnam.com/collections/kinh-te"},
    {"standard_category": "THE_DUC_THE_THAO_GIAI_TRI", "source_category_name": "Thể dục - Thể thao - Giải trí", "url": "https://nhasachphuongnam.com/collections/the-duc-the-thao-giai-tri"},
    {"standard_category": "TAM_LY", "source_category_name": "Tâm lý", "url": "https://nhasachphuongnam.com/collections/tam-ly"},
    {"standard_category": "SACH_HOC_NGOAI_NGU", "source_category_name": "Sách học Ngoại Ngữ", "url": "https://nhasachphuongnam.com/collections/sach-hoc-ngoai-ngu"},
    {"standard_category": "SACH_GIAO_KHOA_THAM_KHAO", "source_category_name": "Sách Giáo khoa & Giáo khoa tham khảo", "url": "https://nhasachphuongnam.com/collections/sach-giao-khoa-giao-khoa-tham-khao"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def now_iso():
    return datetime.now().isoformat()


def clean_price(price_text):
    if not price_text:
        return None
    digits = re.sub(r"[^\d]", "", price_text)
    return int(digits) if digits else None


def save_jsonl_row(file_path, row):
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def get_soup(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        if res.status_code != 200:
            print("Lỗi request:", url, res.status_code)
            return None
        return BeautifulSoup(res.text, "lxml")
    except Exception as e:
        print("Lỗi request exception:", url, e)
        return None
        
def parse_products(soup, category, page_num):
    products = []
    cards = soup.select(
        ".product-item, .product-loop, .pro-loop, .product-block, "
        ".grid__item, .product, .item_product_main, .product-resize"
    )

    if not cards:
        print("Không tìm thấy product card, cần kiểm tra selector")
        return products

    seen_urls_on_page = set()
    for card in cards:
        name_el = card.select_one(
            ".product-name a, .pro-name a, .product-title a, "
            "h3 a, h2 a, a[title]"
        )
        link_el = card.select_one("a[href]")
        price_el = card.select_one(
            ".price, .product-price, .pro-price, .special-price, .current-price, .price-new"
        )
        old_price_el = card.select_one(
            ".compare-price, .old-price, .price-compare, .price-old"
        )
        image_el = card.select_one("img")

        product_name = None
        if name_el:
            product_name = name_el.get("title") or name_el.get_text(strip=True)
        if not product_name:
            continue

        product_url = None
        if link_el and link_el.get("href"):
            product_url = urljoin("https://nhasachphuongnam.com", link_el.get("href"))

        if product_url in seen_urls_on_page:
            continue
        seen_urls_on_page.add(product_url)

        image_url = None
        if image_el:
            image_url = image_el.get("data-src") or image_el.get("data-original") or image_el.get("src")
            if image_url and image_url.startswith("//"):
                image_url = "https:" + image_url
            elif image_url:
                image_url = urljoin("https://nhasachphuongnam.com", image_url)

        price_text = price_el.get_text(" ", strip=True) if price_el else None
        old_price_text = old_price_el.get_text(" ", strip=True) if old_price_el else None

        products.append({
            "source_name": SOURCE_NAME,
            "standard_category": category["standard_category"],
            "source_category_name": category["source_category_name"],
            "product_id": None,
            "product_name": product_name,
            "price": clean_price(price_text),
            "original_price": clean_price(old_price_text),
            "discount": None,
            "rating_average": None,
            "review_count": None,
            "sold_qty": None,
            "product_url": product_url,
            "image_url": image_url,
            "crawl_page": page_num,
            "crawl_time": now_iso(),
        })

    return products


def crawl_phuongnam_category(category, max_page=MAX_PAGE_PER_CATEGORY, output_file=None):
    all_products = []
    empty_page_count = 0

    for page in range(1, max_page + 1):
        url = f"{category['url']}?page={page}"
        soup = get_soup(url)
        if soup is None:
            empty_page_count += 1
            if empty_page_count >= 3:
                break
            continue

        products = parse_products(soup, category, page)
        if not products:
            print(f"Hết data hoặc selector sai: {category['source_category_name']} page {page}")
            empty_page_count += 1
            if empty_page_count >= 2:
                break
            continue

        empty_page_count = 0
        all_products.extend(products)
        for product in products:
            if output_file:
                save_jsonl_row(output_file, product)

        print(f"{category['source_category_name']} | page {page} | lấy {len(products)} sản phẩm")
        time.sleep(REQUEST_SLEEP_SECONDS)

    return all_products


def crawl_phuongnam_all(max_page_per_category=MAX_PAGE_PER_CATEGORY, output_file=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if output_file is None:
        output_file = f"data/raw/phuongnam_raw_{timestamp}.jsonl"
        
    all_products = []
    for category in CATEGORY_CONFIG:
        products = crawl_phuongnam_category(
            category=category,
            max_page=max_page_per_category,
            output_file=output_file,
        )
        all_products.extend(products)
    
    seen = set()
    dedup_products = []

    for p in all_products:
        key = p.get("product_url") or p.get("product_name")
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        dedup_products.append(p)
    
    all_products = dedup_products
    json_file = f"data/raw/phuongnam_raw_{timestamp}.json"
    summary_file = f"data/raw/phuongnam_summary_{timestamp}.json"

    save_json(all_products, json_file)

    summary = {
        "source_name": SOURCE_NAME,
        "run_id": timestamp,
        "total_products": len(all_products),
        "raw_json_file": json_file,
        "raw_jsonl_file": output_file,
        "generated_at": now_iso(),
    }
    
    save_json(summary, summary_file)

    print("Đã lưu JSON:", json_file)
    print("Đã lưu JSONL:", output_file)
    print("Đã lưu summary:", summary_file)
    
    # Upload lên MinIO
    try:
        json_obj = f"raw/{SOURCE_NAME}/phuongnam_raw_{timestamp}.json"
        jsonl_obj = f"raw/{SOURCE_NAME}/phuongnam_raw_{timestamp}.jsonl"
        summary_obj = f"raw/{SOURCE_NAME}/phuongnam_summary_{timestamp}.json"
        upload_to_lake(json_file, json_obj)
        upload_to_lake(output_file, jsonl_obj)
        upload_to_lake(summary_file, summary_obj)
        print(f" Đã upload JSON: {json_obj}")
        print(f" Đã upload JSONL: {jsonl_obj}")
        print(f" Đã upload summary: {summary_obj}")
    except Exception as e:
        print(f" Lỗi upload MinIO: {e}")
    
    return all_products


def save_json(data, file_path):
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    products = crawl_phuongnam_all()
    print("Tổng sản phẩm:", len(products))
   
