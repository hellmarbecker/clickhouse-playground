describe s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

CREATE OR REPLACE TABLE uk_price_paid_py (
    price	    UInt32,
    date	    Date,
    postcode1	LowCardinality(String),
    postcode2	LowCardinality(String),
    type	    Enum('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new	    UInt8,
    duration	Enum('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1	    String,
    addr2	    String,
    street	    LowCardinality(String),
    locality	LowCardinality(String),
    town	    LowCardinality(String),
    district	LowCardinality(String),
    county	    LowCardinality(String)
)
ENGINE = MergeTree()
PRIMARY KEY(postcode1, postcode2)
PARTITION BY toYear(date);

INSERT INTO uk_price_paid_py
SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet')
LIMIT 10000000;

SELECT * FROM uk_price_paid_py;

ALTER TABLE uk_price_paid_py DROP COLUMN date;