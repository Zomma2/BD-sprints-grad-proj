set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

create database if not exists covid_db;
use covid_db;

create table if not exists covid_db.covid_staging
(
  Country   STRING,
  Total_cases   DOUBLE,
  New_cases   DOUBLE,
  Total_Deaths   DOUBLE,
  New_Deaths   DOUBLE,
  Total_Recovered   DOUBLE,
  Active_Cases   DOUBLE,
  Serious   DOUBLE,
  Tot_Cases   DOUBLE,
  Deaths   DOUBLE,
  Total_Tests   DOUBLE,
  Tests   DOUBLE,
  CASES_per_Test   DOUBLE,
  Death_in_Closed_Cases   STRING,
  Rank_by_Testing_rate   DOUBLE,
  Rank_by_Death_rate   DOUBLE,
  Rank_by_Cases_rate   DOUBLE,
  Rank_by_Death_of_Closed_Cases   DOUBLE
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED as TEXTFILE
  LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
  tblproperties ("skip.header.line.count"="1");
  
  create table if not exists covid_db.covid_ds_partitioned
(
  Country   STRING,
  Total_cases   DOUBLE,
  New_cases   DOUBLE,
  Total_Deaths   DOUBLE,
  New_Deaths   DOUBLE,
  Total_Recovered   DOUBLE,
  Active_Cases   DOUBLE,
  Serious   DOUBLE,
  Tot_Cases   DOUBLE,
  Deaths   DOUBLE,
  Total_Tests   DOUBLE,
  Tests   DOUBLE,
  CASES_per_Test   DOUBLE,
  Death_in_Closed_Cases   STRING,
  Rank_by_Testing_rate   DOUBLE,
  Rank_by_Death_rate   DOUBLE,
  Rank_by_Cases_rate   DOUBLE,
  Rank_by_Death_of_Closed_Cases   DOUBLE
  )
  
  PARTITIONED BY (COUNTRY_NAME STRING)
  STORED as TEXTFILE
  LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED';
  
  FROM covid_db.covid_staging
  INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
  select *,Country where Country is not null;
  
  
  create external table covid_db.covid_final_output
  (
    TOP_DEATH  STRING,
    RANK_D     DOUBLE,
    DEATHS     DOUBLE,
    TOP_TEST   STRING,
    RANK_T     DOUBLE,
    TESTS      DOUBLE
  )
   

 ROW FORMAT DELIMITED FIELDS TERMINATED by ','
 STORED as TEXTFILE
 LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';


 insert into covid_db.covid_final_output
 select Country, Rank_by_Death_rate,Deaths, " "," "," "
 from covid_db.covid_staging
 where Rank_by_Death_rate is not null
 order by Rank_by_Death_rate asc
 limit 10;

 insert into covid_db.covid_final_output
 select " "," "," ", Country, Rank_by_Testing_rate,Tests
 from covid_db.covid_staging
 where Rank_by_Testing_rate is not null
 order by Rank_by_Testing_rate asc
 limit 10;
