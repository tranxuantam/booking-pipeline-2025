-- 1. Staging: làm sạch dữ liệu thô
CREATE TABLE IF NOT EXISTS `booking-project-479502.booking_dataset.stg_booking_hotels` AS

WITH raw AS (
  SELECT *
  FROM booking_dataset.raw_hotels_dec2025
  WHERE price_vnd != 'Hết phòng'
    AND price_vnd IS NOT NULL
    AND REGEXP_CONTAINS(price_vnd, r'\d')
),

split_review AS (
  SELECT
    city,
    hotel_name,
    price_vnd,
    rating,
    checkin_date,
    checkout_date,
    day_of_week,
    crawl_time,

    -- Giá sạch
    SAFE_CAST(REGEXP_REPLACE(price_vnd, r'[^\d]', '') AS INT64) AS price_vnd_clean,

    -- Tách dòng
    SPLIT(COALESCE(TRIM(review_text), 'Chưa có'), '\n') AS lines
  FROM raw
)

SELECT
  city,
  TRIM(hotel_name) AS hotel_name,
  price_vnd_clean AS price_vnd,

  -- 1. RATING: NULL → 0.0 (đẹp cho biểu đồ)
  COALESCE(
    SAFE_CAST(REPLACE(REGEXP_EXTRACT(rating, r'\d+[\.,]\d+|\d+'), ',', '.') AS FLOAT64),
    0.0
  ) AS rating,

  -- 2. REVIEW_SCORE_TEXT: NULL hoặc rỗng → "Chưa có đánh giá"
  COALESCE(
    NULLIF(TRIM(lines[SAFE_OFFSET(0)]), ''),
    'Chưa có đánh giá'
  ) AS review_score_text,

  -- 3. REVIEW_COUNT: NULL → 0 (rất quan trọng cho tổng hợp và biểu đồ)
  COALESCE(
    CASE
      WHEN ARRAY_LENGTH(lines) >= 2 THEN
        SAFE_CAST(REGEXP_REPLACE(TRIM(lines[SAFE_OFFSET(1)]), r'[^\d]', '') AS INT64)
      ELSE NULL
    END,
    0
  ) AS review_count,

  -- Ngày giờ
  DATE(checkin_date)   AS checkin_date,
  DATE(checkout_date)  AS checkout_date,
  day_of_week,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawl_time) AS crawl_time

FROM split_review;
