-- 1. Tạo bảng tạm cho fact_hotel_prices
CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.fact_hotel_prices_temp` AS

SELECT
  h.hotel_key,
  din.date_key   AS checkin_date_key,
  dout.date_key  AS checkout_date_key,
  f.price_vnd,
  f.rating,
  f.review_score_text,
  f.review_count,
  din.full_date       AS checkin_date,
  din.day_of_week_vn  AS checkin_day_of_week,
  din.is_weekend      AS checkin_is_weekend,
  din.is_peak_holiday AS checkin_is_peak_holiday,
  dout.full_date      AS checkout_date,
  dout.is_peak_holiday AS checkout_is_peak_holiday,
  f.crawl_time
FROM `booking-project-479502.booking_dataset.stg_booking_hotels` f
JOIN `booking-project-479502.booking_dataset.dim_hotels` h USING (city, hotel_name)
JOIN `booking-project-479502.booking_dataset.dim_date` din  ON f.checkin_date = din.date_key
JOIN `booking-project-479502.booking_dataset.dim_date` dout ON f.checkout_date = dout.date_key;

-- 2. Kiểm tra và gộp dữ liệu cho fact_hotel_prices
BEGIN
  DECLARE fact_hotel_prices_exists BOOL DEFAULT (
    SELECT COUNT(*) > 0 
    FROM `booking-project-479502.booking_dataset.INFORMATION_SCHEMA.TABLES` 
    WHERE table_name = 'fact_hotel_prices'
  );
  
  -- Nếu bảng chính chưa tồn tại, tạo bảng mới
  IF NOT fact_hotel_prices_exists THEN
    CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.fact_hotel_prices` AS
    SELECT * FROM `booking-project-479502.booking_dataset.fact_hotel_prices_temp`;
  ELSE
    -- Nếu bảng chính đã tồn tại, gộp dữ liệu cũ + mới
    CREATE OR REPLACE TABLE `booking-project-479502.booking_dataset.fact_hotel_prices` AS
    SELECT * FROM `booking-project-479502.booking_dataset.fact_hotel_prices`  -- DỮ LIỆU CŨ
    UNION ALL
    SELECT * FROM `booking-project-479502.booking_dataset.fact_hotel_prices_temp`;  -- DỮ LIỆU MỚI
  END IF;
END;

-- 3. Xóa bảng tạm
DROP TABLE IF EXISTS `booking-project-479502.booking_dataset.fact_hotel_prices_temp`;
