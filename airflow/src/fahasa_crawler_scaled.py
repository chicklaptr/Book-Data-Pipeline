import json
import logging
import os
import time
from datetime import datetime
from typing import Any,Dict,List,Optional,Tuple
try:
    from nhap.audit_utils import insert_task_audit,upsert_pipeline_run,finish_pipeline_run,update_task_audit
except:
    def insert_task_audit(*args, **kwargs): pass
    def upsert_pipeline_run(*args, **kwargs): pass
    def finish_pipeline_run(*args, **kwargs): pass
    def update_task_audit(*args, **kwargs): pass
    
from minio import Minio
from src.utils.minio_config import upload_to_lake
from playwright.sync_api import(
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)
def insert_task_audit(*args, **kwargs):
    pass

def upsert_pipeline_run(*args, **kwargs):
    pass

def finish_pipeline_run(*args, **kwargs):
    pass

def update_task_audit(*args, **kwargs):
    pass

#CONFIG

SOURCE_NAME = "fahasa"

# thể loại lớn, và thể loại con có thể click
CATEGORY_CONFIG = {
    "VĂN HỌC": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/van-hoc-trong-nuoc.html",
        "targets": [
            "Tiểu Thuyết",
            "Truyện Ngắn - Tản Văn",
            "Light Novel",
            "Ngôn Tình",
            "Truyện Trinh Thám - Kiếm Hiệp",
            "Huyền Bí - Giả Tưởng - Kinh Dị",
            "Tác Phẩm Kinh Điển",
            "Thơ Ca, Tục Ngữ, Ca Dao, Thành Ngữ",
            "Phóng Sự - Ký Sự - Phê Bình Văn Học",
        ],
    },

    "KINH TẾ": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/kinh-te-chinh-tri-phap-ly.html",
        "targets": [
            "Nhân Vật - Bài Học Kinh Doanh",
            "Quản Trị - Lãnh Đạo",
            "Marketing - Bán Hàng",
            "Phân Tích Kinh Tế",
            "Khởi Nghiệp - Làm Giàu",
            "Tài Chính - Ngân Hàng",
            "Kế Toán - Kiểm Toán - Thuế",
            "Chứng Khoán - Bất Động Sản - Đầu Tư",
        ],
    },

    "TÂM LÝ - KỸ NĂNG SỐNG": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/tam-ly-ky-nang-song.html",
        "targets": [
            "Kỹ Năng Sống",
            "Tâm Lý",
            "Rèn Luyện Nhân Cách",
            "Sách Cho Tuổi Mới Lớn",
            "Chicken Soup - Hạt Giống Tâm Hồn",
        ],
    },

    "NUÔI DẠY CON": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/nuoi-day-con.html",
        "targets": [
            "Cẩm Nang Làm Cha Mẹ",
            "Phương Pháp Giáo Dục Trẻ Các Nước",
            "Phát Triển Kỹ Năng - Trí Tuệ Cho Trẻ",
            "Dinh Dưỡng - Sức Khỏe Cho Trẻ",
            "Dành Cho Mẹ Bầu",
            "Giáo Dục Trẻ Tuổi Teen",
        ],
    },

    "THIẾU NHI": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/thieu-nhi.html",
        "targets": [
            "Truyện Thiếu Nhi",
            "Kiến Thức Bách Khoa",
            "Tô Màu, Luyện Chữ",
            "Kiến Thức - Kỹ Năng Sống Cho Trẻ",
            "Từ Điển Thiếu Nhi",
            "Sách Nói",
            "Tạp Chí Thiếu Nhi",
            "Manga - Comic",
        ],
    },

    "GIÁO KHOA - THAM KHẢO": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/giao-khoa-tham-khao.html",
        "targets": [
            "Sách Giáo Khoa",
            "Sách Tham Khảo",
            "Mẫu Giáo",
            "Sách Giáo Viên",
            "Đại Học",
            "Luyện Thi THPT Quốc Gia",
        ],
    },

    "SÁCH HỌC NGOẠI NGỮ": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/sach-hoc-ngoai-ngu.html",
        "targets": [
            "Tiếng Anh",
            "Tiếng Nhật",
            "Tiếng Hoa",
            "Tiếng Hàn",
            "Tiếng Pháp",
            "Tiếng Đức",
            "Tiếng Việt Cho Người Nước Ngoài",
            "Ngoại Ngữ Khác",
            "Flashcard",
        ],
    },

    "TIỂU SỬ HỒI KÝ": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/tieu-su-hoi-ky.html",
        "targets": [
            "Câu Chuyện Cuộc Đời",
            "Chính Trị",
            "Kinh Tế",
            "Nghệ Thuật - Giải Trí",
            "Thể Thao",
            "Lịch Sử",
        ],
    },

    "CHÍNH TRỊ - PHÁP LÝ - TRIẾT HỌC": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/chinh-tri-phap-ly-triet-hoc.html",
        "targets": [
            "Văn Kiện",
            "Nhân Vật Và Sự Kiện",
            "Triết Học- Lý Luận Chính Trị",
            "Luật - Văn Bản Dưới Luật",
            "Đội - Đoàn - Đảng",
        ],
    },

    "KHOA HỌC KỸ THUẬT": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/khoa-hoc-ky-thuat.html",
        "targets": [
            "Y Học",
            "Tin Học",
            "Khoa Học Vũ Trụ",
            "Cơ Khí",
            "Giáo Dục",
            "Toán Học",
        ],
    },

    "LỊCH SỬ - ĐỊA LÝ - TÔN GIÁO": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/lich-su-dia-ly-ton-giao.html",
        "targets": [
            "Lịch Sử",
            "Tôn Giáo",
            "Địa Lý",
        ],
    },

   "Nữ Công Gia Chánh": {
        "url": "https://www.fahasa.com/sach-trong-nuoc/nu-cong-gia-chanh.html",
        "targets": [
            "Nấu Ăn",
            "Khéo Tay",
            "Làm Đẹp",
            "Món Ăn Bài Thuốc",
            "Mẹo Vặt - Cẩm Nang",
    ],
}
}
# lấy thông số để kết nối với minio đẩy data lên
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT","localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET","data-lake")

#cấu hình cho trang web(trình duyệt nào, và có xem ko khi khởi tạo)
HEADLESS = os.getenv("HEADLESS","false").lower()=="true"
BROWSER_CHANNEL = os.getenv("BROWSER_CHANNEL")

#chia thời gian wait như thời gian load trang, thời gian click, thời gian croll, thời gian chờ reponse,...
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))
SHORT_WAIT_MS = int(os.getenv("SHORT_WAIT_MS", "1000"))
MEDIUM_WAIT_MS = int(os.getenv("MEDIUM_WAIT_MS", "3000"))
LONG_WAIT_MS = int(os.getenv("LONG_WAIT_MS", "5000"))
# Số lần scroll/click thêm trang sau khi vào mỗi subcategory. Tăng để lấy nhiều page hơn.
MAX_SCROLL_PAGES = int(os.getenv("MAX_SCROLL_PAGES", "150"))

#số lần thử lại khi mà không bắt được response hoặc lỗi gì đó
MAX_RETRY = int(os.getenv("MAX_RETRY","3"))

#thư mục lưu trữ ở local
DATA_DIR = os.getenv("DATA_DIR","data/raw")

#từ khóa là bắt response chứa nó (1 là của các sản phẩm theo category, 2 là chi tiết từng sản phẩm )
CATALOG_API_KEYWORDS = ["loadCatalog", "loadproducts"]
FLASHSALE_API_PREFIX = "https://www.fahasa.com/node_api/flashsale/product"

#LOGGING về mặt thông báo để dễ kiểm soát debug
logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

#HELPERS
# 2 hàm lấy thời gian hiện tại có thể là thời gian đã xử lí xong của từng sản phẩm hay category
def now_iso() -> str:
    return datetime.now().isoformat()
def now_str()->str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")
#kiểm tra response có phải dạng json ko 
def safe_json_response(response) ->Optional[Dict[str,Any]]:
    try:
        return response.json()
    except Exception as e:
        logger.warning(f"Không đọc dược response json: {e}")
        return None
#hàm thử lại nếu mà lỗi không bắt được response
def retry(func):
    def wrapper(*args,**kwargs):
        last_error = None
        for attempt in range(1,MAX_RETRY+1):
            try:
                return func(*args,**kwargs)
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Retry {attempt}/{MAX_RETRY} thất bại ở {func.__name__}: {e}"
                )
                if attempt < MAX_RETRY:
                    time.sleep(1)
        raise last_error
    return wrapper

#MINIO
#giả sử đã có dịch vụ minio đc setup, hiện tại ta tạo một client kết nối với service
def create_minio_client()-> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key = MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )   
    return client
#đảm bảo thư mục lưu file data thô tồn tại
def ensure_bucket(client:Minio,bucket_name:str) -> None:
    if not client.bucket_exists(bucket_name):
        logger.info(f"Bucket '{bucket_name}' chưa tồn tại.")
        client.make_bucket(bucket_name)
    else:
        logger.info(f"Bucket '{bucket_name}' đã tồn tại.")
        
#PLAYWRIGHT 
#tạo giả lập một trình duyệt 
# trinhd duyệt
def create_browser(p:Playwright)->Browser:
    launch_options = {"headless": HEADLESS}
    if BROWSER_CHANNEL:
        launch_options["channel"] = BROWSER_CHANNEL
    browser = p.chromium.launch(**launch_options)
    return browser

#phiên làm việc
def create_context(browser:Browser) ->BrowserContext:
    context=browser.new_context()
    context.set_default_timeout(PAGE_TIMEOUT_MS)
    return context

#tương đương các tab
def create_page(context:BrowserContext)-> Page:
    page=context.new_page()
    page.set_default_timeout(PAGE_TIMEOUT_MS)
    return page

#DATA RECORD HELPS  
# thống nhất cấu trúc lưu dữ liệu mỗi response
def build_catalog_record(
    run_id:str,
    category_name:str,
    category_url:str,
    response_url:str,
    response_status:int,
    response_headers:Dict[str,Any],
    body:Dict[str,Any],
)->Dict[str,Any]:
    return {
        "source_name": SOURCE_NAME,
        "run_id": run_id,
        "step": "catalog",
        "source_category_name": category_name,
        "source_category_url": category_url,
        "response_url": response_url,
        "status": response_status,
        "headers": response_headers,
        "crawl_time": now_iso(),
        "is_success": True,
        "error_message": None,
        "body": body,
    }
#khởi tạo những thông có thể không có khi bắt response của chi tiết sản phẩm
def init_product_enrichment_fields(product:Dict[str,Any]) ->None:
    product.setdefault("stock_available", None)
    product.setdefault("sold_qty", None)
    product.setdefault("detail_status", "not_attempted")
    product.setdefault("detail_error", None)
    product.setdefault("detail_crawl_time", None)
    
#CATEGORY / SUBCATEGORY

#nhảy đến một page ban đầu, nó là điều kiện cần để tiếp tục nên phải thử lại nếu không và được
@retry
def goto_page(page:Page,url:str) ->None:
    logger.info(f"Đi tới trang: {url}")
    page.goto(url,wait_until="domcontentloaded")

# trích xuất những category con trong trang category cha đang được trỏ
def extract_targets_from_page(page:Page,allowed_targets:List[str]) ->  List[str]:
    texts = page.locator("a").all_inner_texts()
    #logger.info(texts)
    targets=[]
    for text in texts:
        text = text.strip()
        if text in allowed_targets and text not in targets:
            targets.append(text)
    return targets
#click category con tại page category cha để nhảy sang trang category con và bắt response catalog,nếu không được thì sẽ ko thu data phải thử lại ngay 
@retry
def click_subcategory(page:Page,name:str) ->bool:
    locator = page.locator("a",has_text=name)
    count = locator.count()
    logger.info(f"Đang tìm subcategory '{name}', số locator tìm thấy: {count}")
    
    for i in range(count):
        item = locator.nth(i)
        try:
            if item.is_visible():
                item.scroll_into_view_if_needed()
                page.wait_for_timeout(500)
                item.click(force=True)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(SHORT_WAIT_MS)
                logger.info(f"Click thành công: {name}")
                return True
        except Exception as e:
            logger.warning(f"Lỗi khi thử click '{name}' ở locator {i}: {e}")
    logger.warning(f"Không tìm được thấy phần tử visible ở click cho: {name}")
    return False

#RESPONSE COLLECTION

#PAGINATION / LOAD MORE
# Sau khi click vào subcategory, giả lập người dùng scroll và click phân trang để trang gọi thêm loadCatalog response.
def load_more_catalog_pages(page: Page, max_pages: int = MAX_SCROLL_PAGES) -> None:
    logger.info(f"Bắt đầu click phân trang Fahasa, max_pages={max_pages}")

    for page_num in range(2, max_pages + 1):
        try:
            locator = page.locator("a", has_text=str(page_num))

            if locator.count() == 0:
                logger.info(f"Không tìm thấy nút trang {page_num}, dừng.")
                break

            clicked = False

            for i in range(locator.count()):
                item = locator.nth(i)
                try:
                    text = item.inner_text().strip()
                    if text == str(page_num) and item.is_visible():
                        item.scroll_into_view_if_needed()
                        page.wait_for_timeout(500)
                        item.click(force=True)
                        page.wait_for_timeout(MEDIUM_WAIT_MS)
                        logger.info(f"Đã click trang {page_num}")
                        clicked = True
                        break
                except Exception as e:
                    logger.warning(f"Lỗi khi thử click trang {page_num}: {e}")

            if not clicked:
                logger.info(f"Không click được trang {page_num}, dừng.")
                break

        except Exception as e:
            logger.warning(f"Lỗi khi load trang {page_num}: {e}")
            break
# phần sẽ nghe tại trang đang trỏ và bắt response kiểm tra thỏa mãn điều kiện,nếu ok thì đưa về dạng dict để lưu 
def attach_catalog_response_listener(
    page:Page,
    raw_data:List[Dict[str,Any]],
    seen_keys:set,
    run_id:str,
    current_context:Dict[str,Optional[str]],
) -> None:
    def handle_response(response):
        try:
            if not any(keyword in response.url for keyword in CATALOG_API_KEYWORDS):
                return
            if response.status !=200:
                return 
            category_name = current_context.get("category_name")
            category_url = current_context.get("category_url")
            
            if not category_name or not category_url:
                return
            
            dedup_key =(category_name,response.url)
            if dedup_key in seen_keys:
                return
            body=safe_json_response(response)
            if body is None:
                return
            
            seen_keys.add(dedup_key)
            
            record = build_catalog_record(
                run_id=run_id,
                category_name=category_name,
                category_url=category_url,
                response_url=response.url,
                response_status=response.status,
                response_headers=dict(response.headers),
                body=body,
            )
            raw_data.append(record)
            logger.info(
                f"Đã bắt catalog response | category={category_name} | url={response.url}"
            )
        except Exception as e:
            logger.warning(f"Lỗi khi xử lí response listeners: {e}")
            
    page.on("response",handle_response)
# cái hàm ở trên lấy cái thông tin lấy chạy đúng nếu bắt response theo thứ tự nhưng nếu mà
# xảy ra điều kiện response mất hoặc đến muộn có thể do máy hay mạng thì sai, hàm bên dưới an toàn hơn
#vì mỗi lần bắn chỉ bắt response đúng theo lần đó thôi, an toàn về mặt thứ tự phù hợp logic cần thứ tự
def capture_catalog_response(
    page:Page,
    run_id:str,
    category_name:str,
    category_url:str,
)-> Optional[Dict[str,Any]]:
    try:
        with page.expect_response(
            lambda r: any(keyword in r.url for keyword in CATALOG_API_KEYWORDS) and r.status == 200,
            timeout=10000,
        ) as resp_info:
            clicked=click_subcategory(page,category_name)
            
        if not clicked:
            logger.warning(f"Không click được subcategory: {category_name}")
            return None
        
        response=resp_info.value
        body=safe_json_response(response)
        
        if body is None:
            logger.warning(f"Response của '{category_name}' không đọc được JSON.")
            return None
        
        record=build_catalog_record(
            run_id=run_id,
            category_name=category_name,
            category_url=category_url,
            response_url=response.url,
            response_status=response.status,
            response_headers=dict(response.headers),
            body=body,
        )
        
        logger.info(f"Đã bắt response category trực tiếp | category={category_name} | url={response.url}")
        
        return record
    except PlaywrightTimeoutError:
        print(f"Không bắt được loadCatalog response cho: {category_name}")
        return None
    except Exception as e:
        print(f"Lỗi khi capture catalog response cho '{category_name}': {e}")
        return None
    
#PRODUCT DETAIL ENRICHMENT
# lấy data theo chi tiết của một sản phẩm và cập nhật nhũng trường còn thiếu cho nó đủ 
def enrich_one_product(page:Page,product:Dict[str,Any]) ->None:
    init_product_enrichment_fields(product)

    product_url = product.get("product_url")
    if not product_url:
        product["detail_status"] = "missing_product_url"
        product["detail_error"] = "product_url is missing"
        return
    try:
        with page.expect_response(
            lambda r : FLASHSALE_API_PREFIX in r.url and r.status ==200,
            timeout = 8000,
        ) as resp_info:
            page.goto(product_url,wait_until="domcontentloaded")
            
        resp = resp_info.value
        res = safe_json_response(resp) or {}
        
        product_data = res.get("product",{})
        
        if not product_data:
            product["detail_status"]="no_flashsale_data"
            product["detail_crawl_time"]=now_iso()
            return
        buffer_value = product_data.get("buffer_value", 0)
        total_sold = product_data.get("total_sold")

        product["stock_available"] = 1 if buffer_value > 0 else 0
        product["sold_qty"] = total_sold
        product["detail_status"] = "success"
        product["detail_error"] = None
        product["detail_crawl_time"] = now_iso()
        
    except PlaywrightTimeoutError:
        product["detail_status"] = "no_flashsale_response"
        product["detail_error"] = "Flashsale API response timeout or not triggered"
        product["detail_crawl_time"] = now_iso()

    except Exception as e:
        product["detail_status"] = "error"
        product["detail_error"] = str(e)
        product["detail_crawl_time"] = now_iso()
    
# dựa vào hàm trên lấy data cho tất cả sản phẩm đã thu đuọc cũng như cập nhật trạng thái của nó
def enrich_all_products(page: Page, raw_data: List[Dict[str, Any]]) -> None:
    logger.info("Bắt đầu enrich product detail...")

    total_products = 0
    for record in raw_data:
        body = record.get("body", {})
        product_list = body.get("product_list", [])

        for product in product_list:
            total_products += 1
            enrich_one_product(page, product)
            page.wait_for_timeout(SHORT_WAIT_MS)

    logger.info(f"Hoàn tất enrich detail cho {total_products} sản phẩm.")
    
#SUMMARY
#Tóm tắt những gì đã thu được
def build_summary(raw_data:List[Dict[str,Any]]) ->Dict[str,Any]:
    total_catalog_responses = len(raw_data)
    total_products =0
    detail_success =0
    detail_missing = 0
    detail_error=0
    no_flashsale_data=0
    no_flashsale_response=0
    
    categories =set()
    for record in raw_data:
        categories.add(record.get("source_category_name"))
        
        body = record.get("body",{})
        product_list=body.get("product_list",[])
        total_products+=len(product_list)
        
        for product in product_list:
            status=product.get("detail_status")
            if status == "success":
                detail_success+=1
            elif status == "missing_product_url":
                detail_missing+=1
            elif status =="no_flashsale_data":
                no_flashsale_data+=1
            elif status =="no_flashsale_response":
                no_flashsale_response+=1
            elif status =="error":
                detail_error+=1
    return {
        "source_name": SOURCE_NAME,
        "total_subcategories_collected": len(categories),
        "total_catalog_responses": total_catalog_responses,
        "total_products": total_products,
        "detail_success": detail_success,
        "detail_missing": detail_missing,
        "no_flashsale_data": no_flashsale_data,
        "no_flashsale_response": no_flashsale_response,
        "detail_error": detail_error,
        "generated_at": now_iso(),
    }
    
#STORAGE
#Lưu thông tin thô về mặt toàn bộ dữ liệu ở local
def save_raw_local(raw_data: List[Dict[str, Any]], run_id: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
  
    raw_file = os.path.join(DATA_DIR, f"fahasa_raw_{run_id}.jsonl") 
    
    with open(raw_file, "w", encoding="utf-8") as f:
        for record in raw_data:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
    logger.info(f"Đã lưu raw data dạng JSONL tại: {raw_file}")
    return raw_file

#lưu thông tin thô về mặt tóm tắt dữ liệu ở local
def save_summary_local(summary:Dict[str,Any],run_id:str)->str:
    os.makedirs(DATA_DIR,exist_ok=True)
    summary_file = os.path.join(DATA_DIR, f"fahasa_summary_{run_id}.json")
    with open(summary_file,"w",encoding="utf-8") as f:
        json.dump(summary,f,ensure_ascii=False,indent=2)
    
    logger.info(f"Đã lưu summary local: {summary_file}")
    return summary_file    
        
#upload thông tin thô về mặt toàn bộ dữ liệu lên minio(tầng lưu raw data: dữ liệu lấy được nhiều nhất nhưng chưa xử lí)
def upload_file_to_minio(client:Minio,bucket_name:str,file_path:str,object_name:str)->str:
    client.fput_object(bucket_name,object_name,file_path)
    logger.info(f"Đã upload MinIO: {object_name}")
    return object_name

#MAIN CRAWL LOGIC
#hàm tổng quát tạo logic theo luồng chạy
# tạo môt cái listener tại vị trí trang và khi click nó sẽ bắt response và xử lí lưu data
def crawl_categories(page:Page,run_id:str) -> List[Dict[str,Any]]:
    raw_data:List[Dict[str,Any]] = []
    seen_keys = set()
    current_context ={
        "category_name":None,
        "category_url":None,
    }
    attach_catalog_response_listener(
        page=page,
        raw_data=raw_data,
        seen_keys=seen_keys,
        run_id=run_id,
        current_context=current_context,
    )
    
    # đảm bảo bắt được những response liên quan(có thể failed hoặc success) để dễ kiểm soát
    page.on(
            "request",
            lambda req: logger.info(f"REQUEST: {req.url}")
            if any(keyword in req.url for keyword in CATALOG_API_KEYWORDS) else None
            )
    
    page.on(
        "requestfailed",
        lambda req: logger.warning(f"REQUEST FAILED: {req.url}")
        if any(keyword in req.url for keyword in CATALOG_API_KEYWORDS) else None
    )
    
    
    for main_category_name,conf in CATEGORY_CONFIG.items():
        category_url=conf["url"]
        allowed_targets = conf["targets"]
        
        logger.info(f"===== Bắt đầu category cha: {main_category_name} =====")

        goto_page(page,category_url)
        page.wait_for_timeout(MEDIUM_WAIT_MS)
        
        targets = extract_targets_from_page(page,allowed_targets)
        logger.info(f"Targets sẽ click trong '{main_category_name}': {targets}")

        for target_name in targets:
            current_context["category_name"]=target_name
            current_context["category_url"]=category_url
            
            try:
                goto_page(page,category_url)
                page.wait_for_timeout(SHORT_WAIT_MS)
                
                clicked = click_subcategory(page,target_name)
                if not clicked:
                    logger.warning(f"Không click được subcategory: {target_name}")
                    continue
                
                page.wait_for_timeout(MEDIUM_WAIT_MS)
                load_more_catalog_pages(page, MAX_SCROLL_PAGES)
            except Exception as e:
                logger.warning(f"Lỗi khi crawl subcategory '{target_name}': {e}")
        
    expected_total=sum(len(conf["targets"]) for conf in  CATEGORY_CONFIG.values())
    actual_total = len(raw_data)
    
    if actual_total < expected_total:
        logger.warning(
            f"Thiếu catalog response: expected={expected_total},actual={actual_total}"
            )
    else:
        logger.info(
            f"Đủ catalog response: expected={expected_total},actual={actual_total}"
        )

    return raw_data

#hàm này gọi toàn bộ để phần crawl theo logic chính
def crawl_raw_catalog_data() -> Dict[str,Any]:
    run_id=now_str()
    client=None
    client=create_minio_client()
    ensure_bucket(client,MINIO_BUCKET)
    
    upsert_pipeline_run(
        process_run_id=run_id,
        processed_at=None,
        source_name=SOURCE_NAME,
        raw_file_minio=None,
        total_raw_records=None,
        status="running",
        error_message=None
    )
    insert_task_audit(
        process_run_id=run_id,
        task_name="crawl_data",
        status="running"
    )
    
    with sync_playwright()as p:
        browser=create_browser(p)
        context=create_context(browser)
        page=create_page(context)
        
        try:
            raw_data=crawl_categories(page,run_id)
            # enrich_all_products(page,raw_data)
            summary=build_summary(raw_data)
            raw_file=save_raw_local(raw_data,run_id)
            summary_file=save_summary_local(summary,run_id)
            
            raw_object_name = f"raw/fahasa/fahasa_raw_{run_id}.jsonl"
            summary_object_name = f"raw/fahasa/fahasa_summary_{run_id}.json"
            
            upsert_pipeline_run(
                process_run_id=run_id,
                processed_at=summary.get("generated_at"),
                source_name=SOURCE_NAME,
                raw_file_minio=raw_object_name,
                total_raw_records=summary.get("total_products",0),
                status="running",
                error_message=None
            )
            
            logger.info("===== CRAWL SUMMARY =====")
            logger.info(json.dumps(summary, ensure_ascii=False, indent=2))
            
            update_task_audit(
                process_run_id=run_id,
                task_name="crawl_data",
                status="success",
                records_in=0,
                records_out=summary.get("total_products",0),
                records_rejected=summary.get("detail_error",0)+summary.get("no_flashsale_response",0),
                message=f"subcategories_collected={summary.get('total_subcategories_collected',0)}"
            )
            
            # Upload lên MinIO
            try:
                upload_to_lake(raw_file, raw_object_name)
                upload_to_lake(summary_file, summary_object_name)
                logger.info(f" Đã upload JSON: {raw_object_name}")
                logger.info(f" Đã upload summary: {summary_object_name}")
            except Exception as e:
                logger.warning(f" Lỗi upload MinIO: {e}")
            
            return {
                "run_id": run_id,
                "raw_file": raw_file,
                "summary_file": summary_file,
                "raw_object_name": raw_object_name,
                "summary_object_name": summary_object_name,
                "summary": summary,
            }
        except Exception as e:
            update_task_audit(
                process_run_id=run_id,
                task_name="crawl_data",
                status="failed",
                message=str(e)
            )
            finish_pipeline_run(
                process_run_id=run_id,
                status="failed",
                error_message=str(e)
            )
            raise
        finally:
            browser.close()
#phần đầu để chạy cho cả file
if __name__ == "__main__":
    result = crawl_raw_catalog_data()
    print("\n=== KẾT QUẢ CUỐI ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))
