DROP TABLE IF EXISTS michel_pp_homework.gold_allviews;

CREATE TABLE michel_pp_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://michel-cc-wikidata/datalake/gold_allviews/'
    ) AS
    SELECT 
        article,
        SUM(views) AS total_top_view,
        MIN(rank) AS top_rank,
        COUNT(DISTINCT date) AS ranked_days
    FROM michel_pp_homework.silver_views
    GROUP BY article;
