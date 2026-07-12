import json, os, re, time, requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from src.utils.minio_config import upload_to_lake

SOURCE_NAME = "bookbuy"

MAX_PAGE_PER_CATEGORY = int(os.getenv("MAX_PAGE_PER_CATEGORY", "80"))
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.8"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
DATA_DIR = os.getenv("DATA_DIR", "data/raw")

CATEGORY_CONFIG = [
    {"standard_category": "BOOK", "source_category_name": "Sách - Truyện tranh", "url": "https://bookbuy.vn/sach.html"},
    {"standard_category": "BOOK", "source_category_name": "Phan Thị", "url": "https://bookbuy.vn/nha-cung-cap/phan-thi-sup19/p1?cat=1"},
    {"standard_category": "BOOK", "source_category_name": "Bách Việt", "url": "https://bookbuy.vn/nha-cung-cap/bach-viet-sup16/p1?cat=1"},
    {"standard_category": "BOOK", "source_category_name": "Thái Hà", "url": "https://bookbuy.vn/nha-cung-cap/thai-ha-sup6/p1?cat=1"},
    {"standard_category": "BOOK", "source_category_name": "TGM", "url": "https://bookbuy.vn/nha-cung-cap/tgm-sup27/p1?cat=1"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

def now_iso(): return datetime.now().isoformat()
def now_str(): return datetime.now().strftime("%Y%m%d_%H%M%S")

def clean_price(value):
    if value is None: return None
    digits = re.sub(r"[^\d]", "", str(value).replace("₫", "").replace("đ", ""))
    return int(digits) if digits else None

def ensure_dir(path): os.makedirs(path, exist_ok=True)

def save_json(data, file_path):
    ensure_dir(os.path.dirname(file_path) or ".")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_jsonl_row(file_path, row):
    ensure_dir(os.path.dirname(file_path) or ".")
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def get_soup(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if res.status_code != 200:
            print("Lỗi request:", url, res.status_code)
            return None
        return BeautifulSoup(res.text, "lxml")
    except Exception as e:
        print("Lỗi request exception:", url, e)
        return None

def build_page_url(base_url, page_num):
    if page_num <= 1:
        return base_url
    # URL supplier của Bookbuy có dạng /p1?cat=1
    if re.search(r"/p\d+", base_url):
        return re.sub(r"/p\d+", f"/p{page_num}", base_url)
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}p={page_num}"

def parse_product_card(card, category, page_num):
    img_view = card.select_one(".img-view")
    title_link = card.select_one(".img-view a[href], .t-view a[href]")

    product_name = None
    product_id = None

    if img_view:
        product_name = img_view.get("title")
        product_id = img_view.get("productid")

    if not product_name:
        name_el = card.select_one(".t-view a, a[title]")
        if name_el:
            product_name = name_el.get("title") or name_el.get_text(" ", strip=True)

    if not product_name:
        return None

    product_name = product_name.strip()

    product_url = None
    if title_link and title_link.get("href"):
        product_url = urljoin("https://bookbuy.vn", title_link.get("href"))

    image_url = None
    img_el = card.select_one(".img-view img, img")
    if img_el:
        image_url = (
            img_el.get("data-src")
            or img_el.get("data-original")
            or img_el.get("data-lazy")
            or img_el.get("src")
        )

    if not image_url and img_view:
        style = img_view.get("style") or ""
        m = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
        if m:
            image_url = m.group(1)

    if image_url:
        if image_url.startswith("//"):
            image_url = "https:" + image_url
        else:
            image_url = urljoin("https://bookbuy.vn", image_url)

    price_el = card.select_one(".p-view .price")
    real_price_el = card.select_one(".p-view .real-price")
    sale_off_el = card.select_one(".p-view .sale-off")

    price = clean_price(price_el.get_text(" ", strip=True)) if price_el else None
    original_price = clean_price(real_price_el.get_text(" ", strip=True)) if real_price_el else None

    discount = None
    if sale_off_el:
        m = re.search(r"(\d+)", sale_off_el.get_text(" ", strip=True))
        if m:
            discount = int(m.group(1))

    return {
        "source_name": SOURCE_NAME,
        "standard_category": category["standard_category"],
        "source_category_name": category["source_category_name"],
        "source_category_url": category["url"],
        "product_id": product_id,
        "product_name": product_name,
        "price": price,
        "original_price": original_price,
        "discount": discount,
        "rating_average": None,
        "review_count": None,
        "sold_qty": None,
        "stock_available": None,
        "product_url": product_url,
        "image_url": image_url,
        "crawl_page": page_num,
        "crawl_time": now_iso(),
    }

def parse_products(soup, category, page_num):
    products = []
    cards = soup.select(".center.book-product .product-item, .book-product .product-item, .product-item")
    if not cards:
        links = soup.select("a[href]")
        for link in links:
            parent = link
            for _ in range(3):
                if parent.parent: parent = parent.parent
            cards.append(parent)

    seen = set()
    for card in cards:
        p = parse_product_card(card, category, page_num)
        if not p:
            continue
        key = p.get("product_url") or p.get("product_name")
        if not key or key in seen:
            continue
        seen.add(key)
        products.append(p)
    return products

def crawl_bookbuy_category(category, max_page=MAX_PAGE_PER_CATEGORY, output_file=None):
    all_products, empty_page_count = [], 0
    for page_num in range(1, max_page + 1):
        url = build_page_url(category["url"], page_num)
        print(f"Bookbuy | {category['source_category_name']} | page {page_num}")
        print(url)

        soup = get_soup(url)
        if soup is None:
            empty_page_count += 1
            if empty_page_count >= 3: break
            continue

        products = parse_products(soup, category, page_num)
        if not products:
            print(f"Hết data hoặc selector chưa đúng: {category['source_category_name']} page {page_num}")
            empty_page_count += 1
            if empty_page_count >= 2: break
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        empty_page_count = 0
        all_products.extend(products)
        for p in products:
            if output_file: save_jsonl_row(output_file, p)
        print(f"Lấy {len(products)} sản phẩm")
        time.sleep(REQUEST_SLEEP_SECONDS)

    return all_products

def crawl_bookbuy_all(max_page_per_category=MAX_PAGE_PER_CATEGORY, output_file=None):
    timestamp = now_str()
    if output_file is None:
        output_file = os.path.join(DATA_DIR, f"bookbuy_raw_{timestamp}.jsonl")

    all_products = []
    for category in CATEGORY_CONFIG:
        all_products.extend(crawl_bookbuy_category(category, max_page_per_category, output_file))

    summary_file = os.path.join(DATA_DIR, f"bookbuy_summary_{timestamp}.json")
    save_json({
        "source_name": SOURCE_NAME,
        "run_id": timestamp,
        "total_products": len(all_products),
        "raw_jsonl_file_before_dedup": output_file,  
        "generated_at": now_iso(),
    }, summary_file)

    print("\n===== BOOKBUY DONE =====")
    print("Tổng :", len(all_products))
    print("Đã lưu JSONL trước dedup:", output_file)
    print("Đã lưu summary:", summary_file)
    
    # Upload lên MinIO
    try:
        jsonl_obj = f"raw/{SOURCE_NAME}/bookbuy_raw_{timestamp}.jsonl"
        summary_obj = f"raw/{SOURCE_NAME}/bookbuy_summary_{timestamp}.json"
        upload_to_lake(output_file, jsonl_obj)
        upload_to_lake(summary_file, summary_obj)
        print(f" Đã upload JSONL: {jsonl_obj}")
        print(f" Đã upload summary: {summary_obj}")
    except Exception as e:
        print(f" Lỗi upload MinIO: {e}")
    
    return all_products

if __name__ == "__main__":
    products = crawl_bookbuy_all()
    print("Tổng sản phẩm Bookbuy:", len(products))
