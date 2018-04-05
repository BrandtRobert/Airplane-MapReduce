#!/bin/bash
# cs455/hadoop/mapreduce/busiestairports/BusiestAirportsJob.class
hadoop jar ./dist/airportanalysis.jar \
cs455.hadoop.mapreduce.delays.DelaysJob \
/data/main/ \
/home/cs455/delays/$1
