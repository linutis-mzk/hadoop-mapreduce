# Hadoop MapReduce

A Java program which implements four different queries based on the MapReduce framework, completed as part of the University assignment. The comprises of 2 tables: store_sales and store; approximately 10 million records per table. 
Each query is stored in a separate folder. Based on the Exploratory Data Analysis, we were able to run queries on a specific chunks of data to analyse the performance. 
Bash terminal output can be seen in the log files. The performance of Hadoop was then compared against Hive SQL, resulting in a better performance in every test case.


## SQL Equivalents
#### Query 1a
SELECT ss_store_sk AS store_sk, COALESCE(SUM(ss_net_paid), 0) AS net_paid FROM store_sales WHERE 
ss_sold_date_sk >= 2451146 AND ss_sold_date <= 2451894 GROUP BY ss_store_sk ORDER BY net_paid DESC LIMIT 10;

### Query 1b
SELECT ss_item_sk AS item_sk, COALESCE(SUM(ss_quantity), 0) AS quantity FROM store_sales WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date <= 2451894 GROUP BY ss_item_sk ORDER BY quantity DESC LIMIT 10;

### Query 1c
SELECT ss_sold_date_sk AS sold_date, COALESCE(SUM(ss_net_paid_inc_tax), 0) AS net_paid_inc_tax FROM store_sales WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date <= 2451894 GROUP BY ss_sold_date_sk ORDER BY net_paid_inc_tax DESC LIMIT 10;

### Query 2
SELECT store.s_store_sk AS store_sk, store.s_floor_space AS floor_space, COALESCE(SUM(store_sales.ss_net_paid), 0) AS net_paid FROM store_sales RIGHT OUTER JOIN store ON (STORE.s_store_sk = STORE_SALES.ss_store_sk) WHERE (STORE_SALES.ss_sold_date_sk >= 2451146 AND STORE_SALES.ss_sold_date_sk <= 2451894) OR (STORE_SALES.ss_net_paid IS NULL) GROUP BY STORE.s_store_sk, STORE.s_floor_space ORDER BY STORE.s_floor_space DESC, net_paid DESC LIMIT 10;


