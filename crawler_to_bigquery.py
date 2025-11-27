import asyncio
import random
import json
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from playwright.async_api import async_playwright
import pandas as pd
import os

# ==================== THÊM ĐOẠN NÀY ĐỂ ĐẨY LÊN BIGQUERY ====================
from google.cloud import bigquery

# Đường dẫn đến file key bạn đã tải về và đổi tên thành gcp-key.json
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

# Cấu hình BigQuery (thay booking-project-479502 bằng Project ID của bạn)
PROJECT_ID = "booking-project-479502"   # <<<--- THAY ĐỔI Ở ĐÂY
DATASET_ID = "booking_dataset"
TABLE_ID   = "raw_hotels_dec2025"

# Tạo client BigQuery (sau này chỉ dùng 1 lần)
client = bigquery.Client(project=PROJECT_ID)

# Tạo dataset + table tự động nếu chưa có (chỉ chạy 1 lần đầu)
def init_bigquery_table():
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset {DATASET_ID} sẵn sàng hoặc đã tồn tại")

    schema = [
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("hotel_name", "STRING"),
        bigquery.SchemaField("price_vnd", "STRING"),
        bigquery.SchemaField("rating", "STRING"),
        bigquery.SchemaField("review_text", "STRING"),
        bigquery.SchemaField("checkin_date", "STRING"),
        bigquery.SchemaField("checkout_date", "STRING"),
        bigquery.SchemaField("day_of_week", "STRING"),
        bigquery.SchemaField("crawl_time", "STRING"),
    ]
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"Table {TABLE_ID} đã sẵn sàng!")

# Gọi hàm này 1 lần khi chạy lần đầu
init_bigquery_table()
# ========================================================================

# ================================
# CÀI ĐẶT
# ================================
TARGET_COUNT = 10000  # bạn có thể để 15 để test, sau tăng lên 3000-10000 tuỳ máy
all_data = []
sem = asyncio.Semaphore(15)  # vẫn giữ 15 như cũ

TOP_CITIES = [
    "TP. Hồ Chí Minh", "Hà Nội", "Đà Nẵng", "Phú Quốc", "Hội An",
    "Nha Trang", "Đà Lạt", "Vũng Tàu", "Hạ Long", "Huế",
    "Sa Pa", "Cần Thơ", "Quy Nhơn", "Mũi Né - Phan Thiết", "Hà Giang"
]

# Tạo sẵn tất cả ngày tháng 12/2025
checkin_dates = [(datetime(2025, 12, 1) + timedelta(days=i)).strftime("%Y-%m-%d") 
                 for i in range(30)]

async def crawl_one_hotel(page):
    global all_data
    if len(all_data) >= TARGET_COUNT:
        return True

    async with sem:
        for attempt in range(5):
            try:
                city = random.choice(TOP_CITIES)
                checkin = random.choice(checkin_dates)
                year, month, day = checkin.split('-')
                checkout = datetime(int(year), int(month), int(day)) + timedelta(days=1)
                checkout_str = checkout.strftime("%Y-%m-%d")
                weekday_vn = ["Thứ Hai","Thứ Ba","Thứ Tư","Thứ Năm","Thứ Sáu","Thứ Bảy","Chủ Nhật"][checkout.weekday()]

                url = f"https://www.booking.com/searchresults.vi.html?ss={quote_plus(city)}&checkin={checkin}&checkout={checkout_str}&group_adults=2&no_rooms=1&lang=vi"

                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(random.randint(2000, 4000))
                await page.wait_for_selector('div[data-testid="property-card"]', timeout=15000)

                cards = await page.query_selector_all('div[data-testid="property-card"]')
                if not cards:
                    continue

                hotel_card = random.choice(cards)

                name = await (await hotel_card.query_selector('div[data-testid="title"]')).inner_text()
                price_elem = await hotel_card.query_selector('span[data-testid="price-and-discounted-price"]')
                price = await price_elem.inner_text() if price_elem else "Hết phòng"

                rating_elem = await hotel_card.query_selector('div[data-testid="review-score"] > div:first-child')
                rating = await rating_elem.inner_text() if rating_elem else ""

                review_elem = await hotel_card.query_selector('div[data-testid="review-score"] > div:last-child')
                review = await review_elem.inner_text() if review_elem else "Chưa có"

                record = {
                    "city": city,
                    "hotel_name": name.strip(),
                    "price_vnd": price.strip().replace("\xa0", " "),
                    "rating": rating.strip(),
                    "review_text": review.strip(),
                    "checkin_date": checkin,
                    "checkout_date": checkout_str,
                    "day_of_week": weekday_vn,
                    "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                all_data.append(record)
                print(f"[{len(all_data):5d}/{TARGET_COUNT}] {city:15} | {checkin} → {checkout_str} | {name[:45]:45} | {price}")

                if len(all_data) >= TARGET_COUNT:
                    return True
                return False

            except Exception as e:
                if attempt == 4:
                    print(f"Lỗi sau 5 lần thử: {e}")
                await page.wait_for_timeout(2000)
        return False

async def worker(page):
    while len(all_data) < TARGET_COUNT:
        stop = await crawl_one_hotel(page)
        if stop:
            break
        await asyncio.sleep(random.uniform(1, 3))

async def main():
    global all_data
    os.makedirs("output", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="vi-VN",
            java_script_enabled=True,
        )

        tasks = []
        num_workers = 15
        for _ in range(num_workers):
            page = await context.new_page()
            tasks.append(asyncio.create_task(worker(page)))

        print(f"Khởi động {num_workers} workers song song...")
        await asyncio.gather(*tasks)
        await browser.close()

    # ========================= VẪN LƯU FILE NHƯ CŨ (để backup) =========================
    final_data = all_data[:TARGET_COUNT]
    df = pd.DataFrame(final_data)
    df.to_csv("output/random_hotels_dec2025.csv", index=False, encoding="utf-8-sig")
    print(f"Đã lưu file CSV backup: {len(final_data)} bản ghi")

    # ========================= ĐẨY THẲNG LÊN BIGQUERY (mới thêm) =========================
    print("Đang đẩy dữ liệu lên Google BigQuery Sandbox...")
    job = client.load_table_from_dataframe(
        df, 
        f"{DATASET_ID}.{TABLE_ID}",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"  # nếu chạy lại thì ghi thêm
        )
    )
    job.result()  #ed
    print(f"HOÀN TẤT! Đã đẩy {len(final_data)} bản ghi lên BigQuery")
    print(f"→ Xem ngay tại: https://console.cloud.google.com/bigquery?project={PROJECT_ID}&p={PROJECT_ID}&d={DATASET_ID}&t={TABLE_ID}&page=table")

# CHẠY
if __name__ == "__main__":
    asyncio.run(main())