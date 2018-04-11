#!/bin/bash
# cs455/hadoop/mapreduce/busiestairports/BusiestAirportsJob.class
hadoop jar ./dist/airportanalysis.jar \
cs455.airlines.AirlinesJob \
/data/main/ \
/home/cs455/refactor/$1
