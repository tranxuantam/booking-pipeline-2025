-- 1. Tạo bảng tạm với dữ liệu mới từ raw_hotels_dec2025
CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.stg_booking_hotels_temp` AS

WITH raw AS (
  SELECT *
  FROM `booking-project-479502.booking_dataset.raw_hotels_dec2025`
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
    SAFE_CAST(REGEXP_REPLACE(price_vnd, r'[^\d]', '') AS INT64) AS price_vnd_clean,
    SPLIT(COALESCE(TRIM(review_text), 'Chưa có'), '\n') AS lines
  FROM raw
),

processed_data AS (
  SELECT
    city,
    TRIM(hotel_name) AS hotel_name,
    price_vnd_clean AS price_vnd,
    COALESCE(
      SAFE_CAST(REPLACE(REGEXP_EXTRACT(rating, r'\d+[\.,]\d+|\d+'), ',', '.') AS FLOAT64),
      0.0
    ) AS rating,
    COALESCE(
      NULLIF(TRIM(lines[SAFE_OFFSET(0)]), ''),
      'Chưa có đánh giá'
    ) AS review_score_text,
    COALESCE(
      CASE
        WHEN ARRAY_LENGTH(lines) >= 2 THEN
          SAFE_CAST(REGEXP_REPLACE(TRIM(lines[SAFE_OFFSET(1)]), r'[^\d]', '') AS INT64)
        ELSE NULL
      END,
      0
    ) AS review_count,
    DATE(checkin_date) AS checkin_date,
    DATE(checkout_date) AS checkout_date,
    day_of_week,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', crawl_time) AS crawl_time
  FROM split_review
)

SELECT * FROM processed_data;

-- 2. Kiểm tra xem bảng chính đã tồn tại chưa
BEGIN
  DECLARE table_exists BOOL DEFAULT (
    SELECT COUNT(*) > 0 
    FROM `booking-project-479502.booking_dataset.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = 'stg_booking_hotels'
  );
  
  -- Nếu bảng chính chưa tồn tại, tạo bảng mới
  IF NOT table_exists THEN
    CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.stg_booking_hotels` AS
    SELECT * FROM `booking-project-479502.booking_dataset.stg_booking_hotels_temp`;
  ELSE
    -- Nếu bảng chính đã tồn tại, gộp dữ liệu cũ + mới
    CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.stg_booking_hotels` AS
    SELECT * FROM `booking-project-479502.booking_dataset.stg_booking_hotels`  -- DỮ LIỆU CŨ
    UNION ALL
    SELECT * FROM `booking-project-479502.booking_dataset.stg_booking_hotels_temp`;  -- DỮ LIỆU MỚI
  END IF;
END;

-- 3. Xóa bảng tạm
DROP TABLE IF EXISTS `booking-project-479502.booking_dataset.stg_booking_hotels_temp`;
