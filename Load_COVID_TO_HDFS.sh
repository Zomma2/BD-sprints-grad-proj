#!/bin/bash

hdfs dfs -mkdir -p /user/cloudera/ds/COVID_HDFS_LZ

hdfs dfs -put /home/cloudera/covid_project/landing_zone/COVID_SRC_LZ/covid-19.csv /user/cloudera/ds/COVID_HDFS_LZ
