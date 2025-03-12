-- Step 1: Read Data 1 from Kafka
SELECT
    key AS record_id,
    CAST(value AS STRING) AS raw_json_data,
    event_time
FROM KafkaSourceTopic;

-- Step 2: Read Data 2 from Azure Event Hub
SELECT
    event_id,
    stock_symbol,
    stock_price,
    stock_sentiment,
    event_time AS stock_event_time
FROM AzureEventHubTopic;

-- Step 3: Data Cleaning & Transformation
-- Convert JSON field to structured format for processing
SELECT
    record_id,
    json_value(raw_json_data, '$.title') AS title,
    json_value(raw_json_data, '$.content') AS content,
    json_value(raw_json_data, '$.url') AS url,
    json_value(raw_json_data, '$.published_date') AS published_date,
    stock_symbol,
    stock_price,
    stock_sentiment
FROM (
    SELECT record_id, raw_json_data FROM KafkaSourceTopic
) k
LEFT JOIN (
    SELECT stock_symbol, stock_price, stock_sentiment FROM AzureEventHubTopic
) a
ON json_value(raw_json_data, '$.symbol') = a.stock_symbol;

-- Step 4: Convert to JSON Format
SELECT
    record_id,
    json_object(
        'title', title,
        'content', content,
        'url', url,
        'published_date', published_date,
        'stock_symbol', stock_symbol,
        'stock_price', stock_price,
        'stock_sentiment', stock_sentiment
    ) AS enriched_json_data
FROM cleaned_data;

-- Step 5: Save Data to Azure Blob Storage
INSERT INTO AzureBlobStorageTable
SELECT
    enriched_json_data
FROM enriched_data;

-- Step 6: Save Data to Azure SQL Database
INSERT INTO AzureSQLDatabaseTable
SELECT
    record_id,
    enriched_json_data
FROM enriched_data;

-- Step 7: Save Data to Azure Data Lake Storage
INSERT INTO AzureDataLakeStorageTable
SELECT
    record_id,
    enriched_json_data
FROM enriched_data;
