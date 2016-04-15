# IT_Tools_BigData

## HIVE

### query.hql :

This file is used to perform all the aggregations using hive queries on the given data and gives two files as output precisely for the customer and the product aggregations.

## PYTHON

### weatherproj.py

This file deals with python and is used to fetch weather data for all the departments in France using the ForecastIO API.

## OOZIE SCHEDULING

The following files were used to schedule a job to perform the aggregations and fetch the weather data :

### job.xml

This file defines all the parameters and its values that are used in the hive script, workflow and the coordinator. It also defines the address of the namenode and the jobtracker.

### workflow.xml

This file is used to specify the workflow of the the actions to be performed. In our case we first execute the hive query and save the results in hdfs and then fetch the weather data using python.

### coordinator.xml

This file specifies the scheduling configuration for the job execution. It uses parameters like the start and end date/time of execution and also the frequency of execution in minutes.
