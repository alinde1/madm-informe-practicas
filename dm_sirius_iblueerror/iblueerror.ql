--
-- Create target table
--

CREATE DATABASE IF NOT EXISTS tracking
LOCATION
  's3a://bucket-cdr-main-${hivevar:env}/datalake/tracking.db'
;

CREATE TABLE IF NOT EXISTS tracking.iblueerror
(
    code             CHAR(10)   COMMENT  "Error code",
    es_description   CHAR(100)  COMMENT  "Error description in Spanish",
    en_description   CHAR(100)  COMMENT  "Error description in English"
)
STORED AS parquet
TBLPROPERTIES(
    'PARQUET.COMPRESSION'='SNAPPY'
);
