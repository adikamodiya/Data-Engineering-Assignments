CREATE OR REFRESH STREAMING TABLE cyntexa_dev.medallion_day_6.bronze_sales AS
SELECT 
  *,
  _metadata.file_name AS source_file,
  _metadata.file_modification_time AS ingested_at
FROM STREAM read_files(
  '/Volumes/cyntexa_dev/medallion_day_6/raw/',
  format => 'csv',
  inferSchema => false,
     inferColumnTypes => false
);