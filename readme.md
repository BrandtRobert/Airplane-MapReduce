# MapReduce Airlines Survey

This project surveys airline data from 1987 - 2008. The main focus is on delays. Which carriers cause the most delays, which cities have the most weather related delays, which times of day, days of the week, or months of the year are the worst (or best) for delays.

## Building the Project

The project is built with ant, the build.xml is include in the tar. To build the project type:

```
ant
```

## Running Q1 - Q6

Questions 1 - 6 on the assignment can be answered by running the script run.sh and passing it an argument for the name of the output file.

```
cat run.sh ...

# Run Q1 - Q6
hadoop jar ./dist/airportanalysis.jar \
cs455.airlines.AirlinesJob \
/data/main/ \
/home/final/delays/$1

```

It will output data in the form:

* GeneralDelays - part 0 (Q1/Q2) --> for month delays
* GeneralDelays - part 1 (Q1/Q2) --> for day of week delays
* GeneralDelays - part 2 (Q1/Q2) --> for time of day delays
* MajorHubs (Q3) --> contains top 10 hubs for years 1987 - 2008
* CarriersDelay (Q4)
* PlaneDelays (Q5)
* WeatherDelays (Q6)

## Running Q7

Q7 was a data visualization experiment using map reduce and tableau. It explores three main questions:
1. Which cities have the most cancellations?
2. Which airlines have the most cancellations?
3. What is the primary cause of cancellations?

The data is produced using map reduce, and then analyzed using tableau. The pdf explaining the charts and analysis is contained in the tar file as q7.pdf.

To run q7 use the script q7.sh and pass it a parameter for output directories.

```
cat q7.sh ...

hadoop jar ./dist/airportanalysis.jar \
cs455.visualization.CancellationJob \
/data/main/ \
/home/final/visualization/$1

```
