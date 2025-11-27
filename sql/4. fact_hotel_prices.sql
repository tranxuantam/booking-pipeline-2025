--fact_hotel_prices
CREATE OR REPLACE TABLE booking_dataset.fact_hotel_prices AS
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
FROM booking_dataset.stg_booking_hotels f
JOIN booking_dataset.dim_hotels h USING (city, hotel_name)
JOIN booking_dataset.dim_date din  ON f.checkin_date = din.date_key
JOIN booking_dataset.dim_date dout ON f.checkout_date = dout.date_key;