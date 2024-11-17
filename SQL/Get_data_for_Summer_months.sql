SELECT
  TRI.usertype,
  ZIPSTART.zip_code AS zip_code_start,
  ZIPSTARTNAME.borough AS borough_start,
  ZIPSTARTNAME.neighborhood AS neighborhood_start,
  ZIPEND.zip_code AS zip_code_end,
  ZIPENDNAME.borough AS borough_end,
  ZIPENDNAME.neighborhood AS neighborhood_end,
  DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR) AS start_day,
  DATE_ADD(DATE(TRI.stoptime), INTERVAL 5 YEAR) AS stop_day,
  EXTRACT(MONTH FROM TRI.starttime) AS start_month, -- Extracted month
  WEA.temp AS day_mean_temperature, -- Mean temperature
  WEA.wdsp AS day_mean_wind_speed, -- Mean wind speed
  WEA.prcp AS day_total_precipitation, -- Total precipitation
  -- Group trips into 10 minute intervals
  ROUND(CAST(TRI.tripduration / 60 AS INT64), -1) AS trip_minutes,
  COUNT(TRI.bikeid) AS trip_count
FROM
  `bigquery-public-data.new_york_citibike.citibike_trips` AS TRI
INNER JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPSTART
  ON ST_WITHIN(
    ST_GEOGPOINT(TRI.start_station_longitude, TRI.start_station_latitude),
    ZIPSTART.zip_code_geom)
INNER JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPEND
  ON ST_WITHIN(
    ST_GEOGPOINT(TRI.end_station_longitude, TRI.end_station_latitude),
    ZIPEND.zip_code_geom)
INNER JOIN
  `bigquery-public-data.noaa_gsod.gsod20*` AS WEA
  ON PARSE_DATE("%Y%m%d", CONCAT(WEA.year, WEA.mo, WEA.da)) = DATE(TRI.starttime)
INNER JOIN
  `bi-first.Cyclist.Cyclist_shared_from_team` AS ZIPSTARTNAME
  ON ZIPSTART.zip_code = CAST(ZIPSTARTNAME.zip AS STRING)
INNER JOIN
  `bi-first.Cyclist.Cyclist_shared_from_team` AS ZIPENDNAME
  ON ZIPEND.zip_code = CAST(ZIPENDNAME.zip AS STRING)
WHERE
  -- Filter for summer months (June, July, August)
  EXTRACT(MONTH FROM TRI.starttime) IN (6, 7, 8)
  -- This takes the weather data from one weather station
  AND WEA.wban = '94728' -- NEW YORK CENTRAL PARK
  -- Use data from 2014 and 2015 adding 5 to the above to make it recent
  AND EXTRACT(YEAR FROM DATE(TRI.starttime)) BETWEEN 2014 AND 2015
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14