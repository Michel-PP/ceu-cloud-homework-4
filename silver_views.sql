DROP TABLE IF EXISTS michel_pp_homework.silver_views

CREATE TABLE michel_pp_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://michel-cc-wikidata/datalake/views_silver/'
    ) AS
    SELECT 
        article,
        views,
        rank,
        date
    FROM michel_pp_homework.bronze_views
