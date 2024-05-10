-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Initialization
-- MAGIC Initialize parameters from environment and spark-config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC import pandas as pd
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC #env. parameters
-- MAGIC STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACOUNT_NAME")
-- MAGIC CONTAINER_NAME = os.environ.get("CONTAINER_NAME")
-- MAGIC
-- MAGIC # custom parameters
-- MAGIC INPUT_FOLDER ="input"
-- MAGIC DB_SCHEMA_NAME = "datalake"
-- MAGIC
-- MAGIC # calculated data paths
-- MAGIC INPUT_DATA_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{INPUT_FOLDER}"
-- MAGIC DELTALAKE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{DB_SCHEMA_NAME}"
-- MAGIC
-- MAGIC dbutils.widgets.removeAll() 
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # exporting python variables to SQL variables
-- MAGIC dbutils.widgets.text("INPUT_DATA_PATH", INPUT_DATA_PATH)
-- MAGIC dbutils.widgets.text("DB_SCHEMA_NAME", DB_SCHEMA_NAME)
-- MAGIC dbutils.widgets.text("DELTALAKE_PATH", DELTALAKE_PATH)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 0: Create delta tables based on data in storage account.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create & set use the database schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${DB_SCHEMA_NAME};
USE ${DB_SCHEMA_NAME};


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Manually test the schema and the connection

-- COMMAND ----------

-- uncomment, to visually test creation of the schema
-- SHOW SCHEMAS; 

-- COMMAND ----------

-- uncomment to visually test storage connection
-- SELECT COUNT(*) FROM parquet.`$DATA_PATH/hotel-weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Export hotel & weather data as Delta tables
-- MAGIC It was also asked to *Store final DataMarts and intermediate data ... in provisioned with terraform Storage Account ...* so tables should be external. 
-- MAGIC
-- MAGIC Also, the indermediate tables will also be normal external tables, and not temporary tables or views

-- COMMAND ----------

-- Reading and storing hotel-weather data
CREATE OR REPLACE TABLE hotelweather LOCATION '$DELTALAKE_PATH/hotelweather' AS
SELECT * FROM parquet.`${INPUT_DATA_PATH}/hotel-weather`;


-- COMMAND ----------

-- Reading and storing expedia data
CREATE OR REPLACE TABLE expedia LOCATION '$DELTALAKE_PATH/expedia' AS
SELECT * FROM avro.`${INPUT_DATA_PATH}/expedia`;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can manually check the data structure of the tables

-- COMMAND ----------

-- Uncomment to watch hotelweather structure
-- DESCRIBE hotelweather;

-- COMMAND ----------

-- Uncomment to watch expedia structure
-- DESCRIBE expedia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating a smaller expedia table, with only the required fields
-- MAGIC
-- MAGIC The expedia table has many columns, most of which are unnecessiary for our task. We create a derived deble, which only has the 4 columns, wwhich we need. It might speed up the queries.
-- MAGIC

-- COMMAND ----------

-- Creating the reduced table
CREATE OR REPLACE TABLE expedia_silver LOCATION '$DELTALAKE_PATH/expedia_silver' AS
SELECT id as expedia_id,  hotel_id, srch_ci, srch_co
FROM expedia

-- COMMAND ----------

-- Uncomment to test the data
-- SELECT COUNT(*) FROM expedia_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create a hotels table

-- COMMAND ----------

-- remark: in input database address and name are swapped, we re-swap it
CREATE OR REPLACE TABLE hotels LOCATION '$DELTALAKE_PATH/hotels' AS
SELECT DISTINCT id, name as address, country, city, address as name
FROM hotelweather;


-- COMMAND ----------

-- Uncomment to watch hotels structure
-- DESCRIBE hotels

-- COMMAND ----------

-- uncomment to watch hotels
-- SELECT * FROM hotels

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 1: Top 10 hotels with max absolute temperature difference by month.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create hotel temperature difference ranking data per month

-- COMMAND ----------

-- Uncomment to explain before applying
-- EXPLAIN EXTENDED
CREATE OR REPLACE TABLE hotel_tempr_ranks LOCATION '$DELTALAKE_PATH/hotel_tempr_ranks' AS

SELECT * FROM (

  -- calculating differences in temperatures, and ordering in partitions
  -- using ROW_NUMBER instead or RANK to avoid ties
  SELECT *, max_c-min_c as diff_c, max_f-min_f as diff_f,
  ROW_NUMBER() OVER (partition BY year, month ORDER BY (max_c-min_c) DESC) AS diff_rank
  FROM (

    -- inner query:  determine monthly min and max temperatures by grouping the daily data to monthly
    SELECT id, name, country, city, address, year, month,
      min(avg_tmpr_c) as min_c, max(avg_tmpr_c) as max_c, 
      min(avg_tmpr_f) as min_f, max(avg_tmpr_f) as max_f 
    FROM hotelweather
    GROUP BY id,name,country,city,address, year,month)

) WHERE diff_rank <= 10 -- only first 10 by partition


-- COMMAND ----------

-- Uncomment to visually verify the data
-- SELECT * FROM hotel_tempr_ranks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create the visualizations
-- MAGIC
-- MAGIC As we need separate charts for each month, seaborn will be used with pandas.
-- MAGIC
-- MAGIC First, load the data, and convert it to pandas dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # remark: in input database address and name are swapped, had to re-swap
-- MAGIC
-- MAGIC #load data from sql
-- MAGIC sparkdf_tempr_ranks = spark.sql("SELECT address as name, country, city, year, month, diff_c FROM hotel_tempr_ranks")
-- MAGIC
-- MAGIC #convert to pandas dataframe
-- MAGIC pd_tempr_ranks = sparkdf_tempr_ranks.toPandas()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the chart

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # new fields for better display
-- MAGIC pd_tempr_ranks['monthdate'] =  pd_tempr_ranks['year'].astype(str) + "-" + pd_tempr_ranks["month"].astype(str).str.zfill(2)
-- MAGIC pd_tempr_ranks['fullname'] =  pd_tempr_ranks['name'] + "\n" + pd_tempr_ranks['country'] + " / " + pd_tempr_ranks['city']
-- MAGIC
-- MAGIC # Sort data by tempr difference in descending order
-- MAGIC pd_tempr_ranks = pd_tempr_ranks.sort_values(by='diff_c', ascending=False)
-- MAGIC
-- MAGIC # We want to synchronoze the chart X axis, to be visually more appealing, so need max difference for scaling
-- MAGIC tempr_diff_max_value = pd_tempr_ranks['diff_c'].max()
-- MAGIC
-- MAGIC # we create separate charts in a loop
-- MAGIC for monthdate in pd_tempr_ranks.sort_values(by='monthdate', ascending=True)['monthdate'].unique():
-- MAGIC
-- MAGIC     chart_data = pd_tempr_ranks[pd_tempr_ranks['monthdate'] == monthdate]
-- MAGIC     
-- MAGIC     # Set the figure size
-- MAGIC     plt.figure(figsize=(15, 6))
-- MAGIC
-- MAGIC     # Create horizontal bar chart in Seaborn, all bars same color  
-- MAGIC     sns.barplot(x='diff_c', y='fullname', data=chart_data, orient='h', color = 'lightblue')
-- MAGIC     plt.title(f"TOP temperature difference hotels in {monthdate}, °C", fontsize=16, fontweight='bold')
-- MAGIC     plt.xlabel(None)
-- MAGIC     plt.ylabel(None)
-- MAGIC
-- MAGIC     # Set the same x-axis limits for all charts. Start at 1/3 of the width
-- MAGIC     plt.xlim(-tempr_diff_max_value/2, tempr_diff_max_value)
-- MAGIC     ax = plt.gca()
-- MAGIC     ax.spines['left'].set_position(('axes', 1/3))
-- MAGIC     plt.xticks([])
-- MAGIC
-- MAGIC
-- MAGIC     # Add annotations for each bar
-- MAGIC     for index, value in enumerate(chart_data['diff_c']):
-- MAGIC         plt.text(value, index, str(round(value, 2)), va='center', ha='left', fontsize=12)
-- MAGIC     
-- MAGIC     # Remove the top and right lines
-- MAGIC
-- MAGIC     sns.despine(bottom=True)
-- MAGIC     
-- MAGIC     # Show the plot
-- MAGIC     plt.show()  
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 2: Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The exploratory data analysis showed, that
-- MAGIC * There are only 3 months in the database
-- MAGIC * There are many long bookings, which cover more, than 2 months, so months between start and end may be also needed
-- MAGIC
-- MAGIC As per this task, only those months will be used, which are present in the hotel-weather data, i.e the same 3 months, as in Task 1.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating temporary view for years/months

-- COMMAND ----------

-- temporary view for the months, with first and last day of month
CREATE OR REPLACE TEMPORARY VIEW monthdates AS
SELECT DISTINCT year, month, MAKE_DATE(year,month,1) as month_firstday, last_day(MAKE_DATE(year,month,1)) month_lastday 
FROM hotelweather ORDER BY year, month


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create the aggregated table

-- COMMAND ----------

-- Uncomment to explain before applying
-- EXPLAIN EXTENDED
CREATE OR REPLACE TABLE expedia_hotels_monthly LOCATION '$DELTALAKE_PATH/expedia_hotels_monthly' AS
SELECT * FROM
( -- Aggregate expedia counts per hotel/year/month with rank number
  -- using ROW_NUMBER instead or RANK to avoid ties
  SELECT *, ROW_NUMBER() OVER (partition BY year, month ORDER BY (items_count) DESC) AS rank
  FROM
  ( -- Aggregate expedia counts per hotel/year/month
    SELECT hotel_id, year, month, COUNT(*) as items_count
    FROM (
        -- select each month for hotel/expedia which is relevant, i.e. falls on/between checkin and checkout date
        -- CROSS join is used because of the in-between months, but it's not too expensive, because monthdates is 
        -- very small table (currently 3 rows, but theoretically max. 12 row per year)
        SELECT e.*, md.*
        FROM monthdates md
        CROSS JOIN expedia_silver e
        WHERE e.srch_ci<= md.month_lastday AND e.srch_co>=md.month_firstday
    )
    GROUP BY hotel_id,year,month
  )  
)
WHERE rank<=10


-- COMMAND ----------

-- uncomment, to watch the data as a table
-- SELECT * FROM expedia_hotels_monthly;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create the visualization
-- MAGIC
-- MAGIC As the exported data only contains *hotel_id* for the hotels, we joint it with the *hotels* table to get the name, country and city.
-- MAGIC
-- MAGIC Otherwise, the same steps as at Task 1:
-- MAGIC
-- MAGIC As we need separate charts for each month, seaborn will be used with pandas.
-- MAGIC First, load the data, and convert it to pandas dataframe, then create the charts.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #load data from sql. Joining aggregated table with hotels' individual data
-- MAGIC sparkdf_expedia_monthly_ranks = spark.sql("SELECT h.name, h.country, h.city, h.address, ehm.hotel_id, ehm.year, ehm.month, ehm.items_count \
-- MAGIC                                           FROM hotels h \
-- MAGIC                                           JOIN expedia_hotels_monthly ehm \
-- MAGIC                                           ON h.id = ehm.hotel_id")
-- MAGIC
-- MAGIC
-- MAGIC #convert to pandas dataframe
-- MAGIC pd_expedia_ranks = sparkdf_expedia_monthly_ranks.toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # new fields for better display
-- MAGIC pd_expedia_ranks['monthdate'] =  pd_expedia_ranks['year'].astype(str) + "-" + pd_expedia_ranks["month"].astype(str).str.zfill(2)
-- MAGIC pd_expedia_ranks['fullname'] =  pd_expedia_ranks['name'] + "\n" + pd_expedia_ranks['country'] + " / " + pd_expedia_ranks['city']
-- MAGIC
-- MAGIC # Sort data by tempr difference in descending order
-- MAGIC pd_expedia_ranks = pd_expedia_ranks.sort_values(by='items_count', ascending=False)
-- MAGIC
-- MAGIC # We want to synchronoze the chart X axis, to be visually more appealing, so need max difference for scaling
-- MAGIC item_count_max_value = pd_expedia_ranks['items_count'].max()
-- MAGIC
-- MAGIC # we create separate charts in a loop
-- MAGIC for monthdate in pd_expedia_ranks.sort_values(by='monthdate', ascending=True)['monthdate'].unique():
-- MAGIC
-- MAGIC     chart_data = pd_expedia_ranks[pd_expedia_ranks['monthdate'] == monthdate]
-- MAGIC     
-- MAGIC     # Set the figure size
-- MAGIC     plt.figure(figsize=(15, 6))
-- MAGIC
-- MAGIC     # Create horizontal bar chart in Seaborn, all bars same color  
-- MAGIC     sns.barplot(x='items_count', y='fullname', data=chart_data, orient='h', color = 'lightgreen')
-- MAGIC     plt.title(f"TOP busy hotels in {monthdate} (count)", fontsize=16, fontweight='bold')
-- MAGIC     plt.xlabel(None)
-- MAGIC     plt.ylabel(None)
-- MAGIC
-- MAGIC
-- MAGIC     # Set the same x-axis limits for all charts. Start at 1/3 of the width
-- MAGIC     plt.xlim(-tempr_diff_max_value/2, tempr_diff_max_value)
-- MAGIC     ax = plt.gca()
-- MAGIC     ax.spines['left'].set_position(('axes', 1/3))
-- MAGIC     plt.xticks([])
-- MAGIC
-- MAGIC
-- MAGIC     # Set the same x-axis limits for all charts. Start at 1/3 of the width
-- MAGIC     plt.xlim(-item_count_max_value/2, item_count_max_value)
-- MAGIC     ax = plt.gca()
-- MAGIC     ax.spines['left'].set_position(('axes', 1/3))
-- MAGIC     plt.xticks([])
-- MAGIC
-- MAGIC     # Add annotations for each bar
-- MAGIC     for index, value in enumerate(chart_data['items_count']):
-- MAGIC         plt.text(value, index, str(value), va='center', ha='left', fontsize=12)
-- MAGIC     
-- MAGIC     # Remove the top and right lines
-- MAGIC     sns.despine(bottom=True)
-- MAGIC     
-- MAGIC     # Show the plot
-- MAGIC     plt.show()  
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 3: For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC The exploratory data analysis showed, that many weather data are missing, so maybe there are just 1-3 weather day data for a longer period of stay.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create joined hotel/weather - expedia data
-- MAGIC
-- MAGIC In the join, we take into consideration, that weather date should be between *checkin* and *checkout* days, and the difference between these days should be greater than 7
-- MAGIC

-- COMMAND ----------

-- Uncomment to explain before applying
-- EXPLAIN EXTENDED

-- Create table for joined extended stay weather - expedia data
CREATE OR REPLACE TABLE hotels_expedia_joined LOCATION '$DELTALAKE_PATH/hotels_expedia_joined' AS
SELECT 
e.expedia_id,  e.hotel_id ,e.srch_ci, e.srch_co, hw.avg_tmpr_c, hw.avg_tmpr_f, hw.wthr_date,hw.year,hw.month,hw.day
FROM hotelweather hw
INNER JOIN
expedia_silver e
ON hw.id = e.hotel_id
WHERE DATEDIFF(e.srch_co, e.srch_ci)>7
AND hw.wthr_date>= e.srch_ci
AND hw.wthr_date<= e.srch_co
ORDER BY e.hotel_id


-- COMMAND ----------

-- uncomment to check data
--select * from hotels_expedia_joined

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Calculating weather trends for the extended stays

-- COMMAND ----------

-- Uncomment to explain before applying
-- EXPLAIN EXTENDED

-- Create table for joined extended stay data
CREATE OR REPLACE TABLE extended_stay_weather LOCATION '$DELTALAKE_PATH/extended_stay_weather' AS
SELECT 
h.*, 
  h2.avg_tmpr_c as tmpr_c_first, h3.avg_tmpr_c as tmpr_c_last, h3.avg_tmpr_c-h2.avg_tmpr_c as tmpr_c_diff,
  h2.avg_tmpr_f as tmpr_f_first, h3.avg_tmpr_f as tmpr_f_last, h3.avg_tmpr_f-h2.avg_tmpr_f as tmpr_f_diff
FROM
(
  -- SELECTING first and last weather date for individual booking. It's needed, because 
  -- in many cases there are no weather data on the checkin and checkout date
  SELECT expedia_id, hotel_id, srch_ci, srch_co, MIN(wthr_date) as first_date, MAX(wthr_date) AS last_date, 
      AVG(avg_tmpr_c) as full_avg_c,  AVG(avg_tmpr_f) as full_avg_f
  FROM hotels_expedia_joined
  GROUP BY expedia_id, hotel_id, srch_ci, srch_co
) h
JOIN hotels_expedia_joined h2 
ON h.expedia_id=h2.expedia_id AND h.first_date = h2.wthr_date -- for weather data for earliest day
JOIN hotels_expedia_joined h3
ON h.expedia_id=h3.expedia_id AND h.last_date = h3.wthr_date -- -- for weather data for latest day






-- COMMAND ----------

-- uncomment to check data
-- SELECT * FROM extended_stay_weather;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visualize data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #load data from sql.
-- MAGIC sparkdf_exdended_stay_data = spark.sql("SELECT * FROM extended_stay_weather")
-- MAGIC
-- MAGIC #convert to pandas dataframe
-- MAGIC pd_exdended_stay = sparkdf_exdended_stay_data.toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create charts
-- MAGIC
-- MAGIC We create a scatterplow, which shows the average temperature / change of temperature relation, and two histograms.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create a scatterplot 
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC sns.scatterplot(x='full_avg_c', y='tmpr_c_diff', data=pd_exdended_stay, alpha=0.2)
-- MAGIC plt.title(f"Weather trend for extended stays (scatterplot)", fontsize=16, fontweight='bold')
-- MAGIC plt.xlabel('Average temperature during the stay (°C)')
-- MAGIC plt.ylabel('Temperature difference (°C)')
-- MAGIC
-- MAGIC # Remove the top and right lines
-- MAGIC sns.despine()
-- MAGIC
-- MAGIC # Show the plot
-- MAGIC plt.show()  
-- MAGIC
-- MAGIC # Create a histogramm 
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC sns.histplot(x='full_avg_c', data=pd_exdended_stay, bins=10)
-- MAGIC plt.title(f"Histogram for average temperature during extended stays", fontsize=16, fontweight='bold')
-- MAGIC plt.xlabel('°C')
-- MAGIC plt.ylabel('Count')
-- MAGIC sns.despine()
-- MAGIC plt.show() 
-- MAGIC
-- MAGIC # Create a histogramm 
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC sns.histplot(x='tmpr_c_diff', data=pd_exdended_stay, bins=10)
-- MAGIC plt.title(f"Histogram for temperature change during extended stays", fontsize=16, fontweight='bold')
-- MAGIC plt.xlabel('Temperature change(°C)')
-- MAGIC plt.ylabel('Count')
-- MAGIC sns.despine()
-- MAGIC plt.show() 
-- MAGIC
