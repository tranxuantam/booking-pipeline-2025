-- dim_date
CREATE OR REPLACE TABLE booking_dataset.dim_date AS

WITH all_dates AS (
  -- Lấy tất cả ngày checkin + checkout
  SELECT DISTINCT checkin_date  AS date_key, day_of_week AS day_of_week_vn FROM booking_dataset.stg_booking_hotels
  UNION ALL
  SELECT DISTINCT checkout_date AS date_key, day_of_week AS day_of_week_vn FROM booking_dataset.stg_booking_hotels
),

clean_date AS (
  SELECT 
    date_key,
    -- Đảm bảo mỗi ngày chỉ có 1 thứ duy nhất (lấy cái đầu tiên gặp)
    ANY_VALUE(day_of_week_vn) AS day_of_week_vn
  FROM all_dates
  GROUP BY date_key
)

SELECT
  date_key,
  FORMAT_DATE('%Y-%m-%d', date_key)                  AS full_date,
  EXTRACT(DAY FROM date_key)                         AS day_of_month,
  EXTRACT(MONTH FROM date_key)                       AS month,
  EXTRACT(YEAR FROM date_key)                        AS year,
  day_of_week_vn,
  day_of_week_vn IN ('Thứ Bảy', 'Chủ Nhật')          AS is_weekend,
  date_key IN ('2025-12-24','2025-12-25','2025-12-31') AS is_peak_holiday
FROM clean_date
ORDER BY date_key;