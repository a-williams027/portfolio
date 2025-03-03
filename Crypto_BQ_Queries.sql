# update time to snapshotdatetime for BTC
CREATE TABLE IF NOT EXISTS SampleOSUDataSet.BTC_HistwithTime   
AS    
SELECT *,     
 TIMESTAMP_SECONDS(time) AS snapshotdatetime 
FROM SampleOSUDataSet.BTC_HistData;

# update time to snapshotdatetime for ETH
CREATE TABLE IF NOT EXISTS SampleOSUDataSet.ETHHistwithTime   
AS    
SELECT *,     
 TIMESTAMP_SECONDS(time) AS snapshotdatetime 
FROM SampleOSUDataSet.ETHHistData;

# Add the BTC currency symbol to the column 'conversionSymbol' in the BTC dataset with human readable time
UPDATE `my-test-project-for-osu-435622.SampleOSUDataSet.BTC_HistwithTime`
SET conversionSymbol = 'BTC'
WHERE TRUE; 

# Add the ETH currency symbol to the column 'conversionSymbol' in the ETH dataset with human readable time
UPDATE `my-test-project-for-osu-435622.SampleOSUDataSet.ETHHistwithTime`
SET conversionSymbol = 'ETH'
WHERE TRUE; 

# Merge the datasets for BTC and ETH together to create the Mini Project dataset and rename the column 'conversionSymbol' to 'currencySymbol'
CREATE OR REPLACE TABLE `my-test-project-for-osu-435622.SampleOSUDataSet.MiniProjectTable` AS
SELECT
  time,
  high,
  low,
  open,
  volumefrom,
  volumeto,
  close,
  conversionType,
  conversionSymbol AS currencySymbol,
  snapshotdatetime
FROM
  `my-test-project-for-osu-435622.SampleOSUDataSet.BTC_HistwithTime`

UNION ALL

SELECT
  time,
  high,
  low,
  open,
  volumefrom,
  volumeto,
  close,
  conversionType,
  conversionSymbol AS currencySymbol,
  snapshotdatetime
FROM
  `my-test-project-for-osu-435622.SampleOSUDataSet.ETHHistwithTime`;


# Create feature engineering columns for average daily price, day of the week, daily price return based on closing price and estimated moving average for 20 days
CREATE OR REPLACE TABLE `my-test-project-for-osu-435622.SampleOSUDataSet.MiniProjectTableWithFeatures` AS
WITH Base_Features AS (
  SELECT
    *,
    -- Calculate avg_daily_price as the average of high and low
    (high + low) / 2 AS avg_daily_price,

    -- Extract day of the week (Monday, Tuesday, etc.)
    FORMAT_TIMESTAMP('%A', snapshotdatetime) AS day_of_week,

    -- Calculate the daily price return based on the closing price
    ( (close - LAG(close) OVER (PARTITION BY currencySymbol ORDER BY snapshotdatetime)) 
        / LAG(close) OVER (PARTITION BY currencySymbol ORDER BY snapshotdatetime) ) AS price_return,

    -- Generate row number to be used for EMA calculation
    ROW_NUMBER() OVER (PARTITION BY currencySymbol ORDER BY snapshotdatetime) AS row_num
  FROM
    `my-test-project-for-osu-435622.SampleOSUDataSet.MiniProjectTable`
),
EMA_Calculation AS (
  SELECT
    *,
    -- Calculate the Exponential Moving Average (EMA) for the close price using the row_num
    SUM(close * EXP(row_num / -20.0)) 
      OVER (PARTITION BY currencySymbol ORDER BY snapshotdatetime ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) 
      / SUM(EXP(row_num / -20.0)) 
      OVER (PARTITION BY currencySymbol ORDER BY snapshotdatetime ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ema_20
  FROM
    Base_Features
)

-- Now, select all original columns plus the newly added features
SELECT
  *
FROM
  EMA_Calculation;


# run query to view 10 results of new dataframe
SELECT *
FROM `my-test-project-for-osu-435622.SampleOSUDataSet.MiniProjectTableWithFeatures`
LIMIT 10;


