-- dim_hotels
CREATE OR REPLACE TABLE booking_dataset.dim_hotels AS

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
  FROM booking_dataset.stg_booking_hotels
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