SELECT
  raw:"objectId"::STRING AS object_id,
  raw:"g_magnitude"::FLOAT AS g_magnitude,
  raw:"r_magnitude"::FLOAT AS r_magnitude,
  raw:"first_detection"::FLOAT AS jd_first,
  raw:"latest_detection"::FLOAT AS jd_latest,
  raw:"utc_first_detection"::TIMESTAMP AS utc_first_detection,
  raw:"utc_latest_detection"::TIMESTAMP AS utc_latest_detection,
  load_timestamp
FROM {{ source('ztf_data', 'ztf_alerts_raw') }}
