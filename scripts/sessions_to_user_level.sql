
WITH sessions_2023 AS (
    SELECT *
    FROM sessions s
    WHERE s.session_start > '2023-01-04'
),

-- This CTE returns the IDs of all users with more than 7 sessions in 2023
filtered_users AS (
    SELECT user_id, COUNT(*) 
    FROM sessions_2023 
    GROUP BY user_id
    HAVING COUNT(*) > 7
),

-- This is our main session base table
-- It joins Sessions with all available user and trip information
-- We made sure to limit the sessions according to Elena's recommendations

session_base AS (
    SELECT 
        s.session_id, s.user_id, s.trip_id, s.session_start, s.session_end, 
        EXTRACT(EPOCH FROM s.session_end - s.session_start) AS session_duration, 
        s.page_clicks, s.flight_discount, s.flight_discount_amount, 
        s.hotel_discount, s.hotel_discount_amount, s.flight_booked, 
        s.hotel_booked, s.cancellation, u.birthdate, u.gender, u.married, 
        u.has_children, u.home_country, u.home_city, u.home_airport, 
        u.home_airport_lat, u.home_airport_lon, u.sign_up_date,
        f.origin_airport, f.destination, f.destination_airport, f.seats, 
        f.return_flight_booked, f.departure_time, f.return_time, 
        f.checked_bags, f.trip_airline, f.destination_airport_lat, 
        f.destination_airport_lon, f.base_fare_usd, h.hotel_name, 
        CASE WHEN h.nights < 0 THEN 1 ELSE h.nights END AS nights, 
        h.rooms, h.check_in_time, h.check_out_time, 
        h.hotel_per_room_usd AS hotel_price_per_room_night_usd
    FROM sessions_2023 s
    LEFT JOIN users u ON s.user_id = u.user_id
    LEFT JOIN flights f ON s.trip_id = f.trip_id
    LEFT JOIN hotels h ON s.trip_id = h.trip_id
    WHERE s.user_id IN (SELECT user_id FROM filtered_users)
),

-- This CTE returns the IDs of all trips that have been canceled through a session
canceled_trips AS (
    SELECT DISTINCT trip_id
    FROM session_base
    WHERE cancellation = TRUE
),

-- This is our second base table to aggregate later
-- It is derived from our session_base table, but we focus on valid trips
not_canceled_trips AS (
    SELECT *
    FROM session_base
    WHERE trip_id IS NOT NULL
    AND trip_id NOT IN (SELECT trip_id FROM canceled_trips)
),

-- Aggregating user behavior into metrics (browsing behavior)
user_base_session AS (
    SELECT 
        user_id, 
        SUM(page_clicks) AS num_clicks, 
        COUNT(DISTINCT session_id) AS num_sessions, 
        AVG(session_duration) AS avg_session_duration,
  			AVG(checked_bags) AS avg_bags
    FROM session_base
    GROUP BY user_id
),

-- Aggregating user behavior into travel metrics (valid trips only)
user_base_trip AS (
    SELECT 
        user_id, 
        COUNT(DISTINCT trip_id) AS num_trips, 
        SUM(
            CASE 
                WHEN (flight_booked = TRUE) AND (return_flight_booked = TRUE) THEN 2 
                WHEN flight_booked = TRUE THEN 1 
                ELSE 0 
            END
        ) AS num_flights,
 -- discount_flight_proportion
  			SUM(
            CASE 
                WHEN (flight_booked = TRUE) AND (flight_discount = TRUE) THEN 1 
                ELSE 0 
            END) :: numeric (5,2)
          /SUM(
            CASE 
                WHEN (flight_booked = TRUE) AND (return_flight_booked = TRUE) THEN 2 
                WHEN flight_booked = TRUE THEN 1 
             END) AS discount_flight_proportion, 
--average_flight_discount
  			AVG (flight_discount_amount) AS average_flight_discount,
 --average_dollars_spent
  			AVG(flight_discount_amount*base_fare_usd) AS ADS,
        SUM(
            CASE 
                WHEN (flight_booked = TRUE) AND (return_flight_booked = TRUE) THEN 2 
                WHEN flight_booked = TRUE THEN 1 
                ELSE 0 
            END
        )
 -- average_flight_discount
        COALESCE(
            SUM(
                (hotel_price_per_room_night_usd * nights * rooms) * 
                (1 - COALESCE(hotel_discount_amount, 0))
            ), 
            0
        ) AS money_spent_hotel,
        AVG(EXTRACT(DAY FROM departure_time - session_end)) AS time_after_booking,
        AVG(haversine_distance(
            home_airport_lat, home_airport_lon, 
            destination_airport_lat, destination_airport_lon
        )) AS avg_km_flown
    FROM not_canceled_trips
    GROUP BY user_id
)

-- For our final user table, we join session metrics, trip metrics, and general user information
-- Using a LEFT JOIN ensures a row for each user with 7+ browsing sessions in 2023
SELECT 
    -- Columns from user_base_session
    b.user_id, 
    COALESCE(b.num_clicks, 0) AS num_clicks, 
    COALESCE(b.num_sessions, 0) AS num_sessions, 
    COALESCE(b.avg_session_duration, 0) AS avg_session_duration, 
    COALESCE(b.avg_bags, 0) AS avg_bags,
    
    -- Columns from users
    COALESCE(EXTRACT(YEAR FROM AGE(u.birthdate)), 0) AS age, 
    u.gender, 
    u.married, 
    u.has_children, 
    u.home_country, 
    u.home_city, 
    u.home_airport,
    
    -- Columns from user_base_trip
    COALESCE(t.num_trips, 0) AS num_trips, 
    COALESCE(t.num_flights, 0) AS num_flights, 
    COALESCE(t.money_spent_hotel, 0) AS money_spent_hotel, 
    COALESCE(t.time_after_booking, 0) AS time_after_booking, 
    COALESCE(t.avg_km_flown, 0) AS avg_km_flown
		COALESCE (t.average_flight_discount, 0) AS avg_flight_discount
    COALESCE (t.discount_flight_proportion, 0) AS discount_flight_proportion
    COALESCE (t.ADS, 0) AS ads
FROM user_base_session b
LEFT JOIN users u ON b.user_id = u.user_id
LEFT JOIN user_base_trip t ON b.user_id = t.user_id;






