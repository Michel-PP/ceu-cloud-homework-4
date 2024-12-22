DROP TABLE IF EXISTS michel_pp_homework.bronze_views;

CREATE EXTERNAL TABLE
michel_pp_homework.bronze_views (
    article STRING,
    views INT,
    rank INT,
    date DATE,
    retrieved_at STRING) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://michel-cc-wikidata/datalake/views/';
