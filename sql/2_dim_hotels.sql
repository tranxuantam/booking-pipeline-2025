-- dim_hotels
-- 1. Tạo bảng tạm cho dim_hotels
CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.dim_hotels_temp` AS

WITH ranked AS (
  SELECT
    city,
    hotel_name,
    rating,
    review_score_text,
    review_count,
    crawl_time,
    
    -- Xếp hạng theo thời gian crawl mới nhất
    ROW_NUMBER() OVER (
      PARTITION BY city, hotel_name 
      ORDER BY crawl_time DESC
    ) AS rn
  FROM `booking-project-479502.booking_dataset.stg_booking_hotels`
)

SELECT
  ROW_NUMBER() OVER (ORDER BY city, hotel_name) AS hotel_key,
  city,
  hotel_name,
  rating           AS latest_rating,
  review_score_text AS latest_review_text,
  review_count     AS latest_review_count
FROM ranked
WHERE rn = 1;

-- 2. Kiểm tra và gộp dữ liệu cho dim_hotels
BEGIN
  DECLARE dim_hotels_exists BOOL DEFAULT (
    SELECT COUNT(*) > 0 
    FROM `booking-project-479502.booking_dataset.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = 'dim_hotels'
  );
  
  -- Nếu bảng chính chưa tồn tại, tạo bảng mới
  IF NOT dim_hotels_exists THEN
    CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.dim_hotels` AS
    SELECT * FROM `booking-project-479502.booking_dataset.dim_hotels_temp`;
  ELSE
    -- Nếu bảng chính đã tồn tại, gộp dữ liệu cũ + mới
    -- Giữ hotel_key cũ cho các khách sạn đã có, thêm hotel_key mới cho khách sạn mới
    CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.dim_hotels` AS
    WITH combined_data AS (
      -- Dữ liệu cũ: giữ nguyên hotel_key
      SELECT 
        hotel_key,
        city,
        hotel_name,
        latest_rating,
        latest_review_text,
        latest_review_count
      FROM `booking-project-479502.booking_dataset.dim_hotels`
      
      UNION ALL
      
      -- Dữ liệu mới: chỉ lấy khách sạn chưa có trong bảng cũ
      SELECT 
        NULL AS hotel_key,  -- Sẽ được đánh số lại
        city,
        hotel_name,
        latest_rating,
        latest_review_text,
        latest_review_count
      FROM `booking-project-479502.booking_dataset.dim_hotels_temp` t
      WHERE NOT EXISTS (
        SELECT 1 FROM `booking-project-479502.booking_dataset.dim_hotels` d
        WHERE d.city = t.city AND d.hotel_name = t.hotel_name
      )
    )
    SELECT
      ROW_NUMBER() OVER (ORDER BY city, hotel_name) AS hotel_key,
      city,
      hotel_name,
      latest_rating,
      latest_review_text,
      latest_review_count
    FROM combined_data;
  END IF;
END;

-- 3. Xóa bảng tạm
DROP TABLE IF EXISTS `booking-project-479502.booking_dataset.dim_hotels_temp`;
