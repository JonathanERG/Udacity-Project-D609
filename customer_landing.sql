CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    customername STRING,
    email STRING,
    phone STRING,
    birthday STRING,
    serialnumber STRING,
    registrationdate BIGINT,
    lastupdatedate BIGINT,
    sharewithresearchasofdate BIGINT,
    sharewithpublicasofdate BIGINT,
    sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://stedi-d609/customer_landing/';