-- Total successful revenue for the last 7 days.
SELECT
    SUM(payment_amount) AS successful_revenue_inr,
    COUNT(DISTINCT booking_id) AS successful_bookings
FROM bms.enriched_transactions
WHERE payment_status = 'SUCCESS'
  AND booking_ts >= DATEADD(day, -7, GETDATE());

-- Booking to successful-payment conversion rate for the last 7 days.
SELECT
    COUNT(DISTINCT booking_id) AS total_joined_bookings,
    COUNT(DISTINCT CASE WHEN payment_status = 'SUCCESS' THEN booking_id END) AS successful_paid_bookings,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN payment_status = 'SUCCESS' THEN booking_id END)
        / NULLIF(COUNT(DISTINCT booking_id), 0),
        2
    ) AS successful_payment_rate_percent
FROM bms.enriched_transactions
WHERE booking_ts >= DATEADD(day, -7, GETDATE());

-- Top cities by successful revenue.
SELECT
    city,
    SUM(payment_amount) AS revenue_inr,
    COUNT(DISTINCT booking_id) AS bookings
FROM bms.enriched_transactions
WHERE payment_status = 'SUCCESS'
  AND booking_ts >= DATEADD(day, -7, GETDATE())
GROUP BY city
ORDER BY revenue_inr DESC
LIMIT 10;

-- Top movies by ticket sales.
SELECT
    movie_name,
    language,
    SUM(seat_count) AS tickets_sold,
    SUM(payment_amount) AS revenue_inr
FROM bms.enriched_transactions
WHERE payment_status = 'SUCCESS'
  AND booking_ts >= DATEADD(day, -7, GETDATE())
GROUP BY movie_name, language
ORDER BY tickets_sold DESC
LIMIT 10;

-- Payment failures by provider and reason.
SELECT
    payment_provider,
    failure_reason,
    COUNT(*) AS failed_payments
FROM bms.enriched_transactions
WHERE payment_status = 'FAILED'
  AND booking_ts >= DATEADD(day, -7, GETDATE())
GROUP BY payment_provider, failure_reason
ORDER BY failed_payments DESC;

