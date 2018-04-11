#!/bin/bash
# Run Q1 - Q6
hadoop jar ./dist/airportanalysis.jar \
cs455.airlines.AirlinesJob \
/data/main/ \
/home/final/delays/$1