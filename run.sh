#!/bin/bash
# Run Q1 - Q6
hadoop jar ./dist/airportanalysis.jar \
cs455.airlines.AirlinesJob \
/data/main/ \
/home/cs455/refactor/$1