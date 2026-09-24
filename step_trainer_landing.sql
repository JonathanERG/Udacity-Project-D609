CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorreadingtime BIGINT,
    serialnumber STRING,
    distancefromobject INT
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://stedi-d609/step_trainer_landing/';