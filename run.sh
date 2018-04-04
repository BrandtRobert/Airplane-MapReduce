#!/bin/bash
# cs455/hadoop/mapreduce/busiestairports/BusiestAirportsJob.class
hadoop jar ./dist/airportanalysis.jar \
cs455.hadoop.mapreduce.busiestairports.BusiestAirportsJob \
/data/main/ \
/home/cs455/busiestairports/$1
