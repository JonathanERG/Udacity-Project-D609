CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    timestamp BIGINT,
    user STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://stedi-d609/accelerometer_landing/';