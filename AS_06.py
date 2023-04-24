# University:  Middle Tennessee State University - MTSU
# Class:       CSCI 7400 - Assignment 06
# By:          Richard Hoehn (PhD Student)
# Due Date:    2023-04-07
# Desc:        This assignment is to learn and demonstrate student understanding of some basic spark operations including Transformations and Actions.
# Links:       * Spark:     https://spark.apache.org/docs/3.1.3/api/python/index.html
#              * GIT:       https://github.com/richardhoehn/mtsu.csci.7400.assignment06
# Exec:        spark-submit --master local ./AS_06.py

from pyspark.sql import SparkSession
import AS_06_Helper as Helper

# Data File for Assingment 06
dataFile = './AS_Data.txt'

spark = SparkSession.builder.appName('Assignment06').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

rdd_data = spark.sparkContext.textFile(dataFile)
Helper.printRdd('Start', 'rdd_data', rdd_data)

rdd_split = rdd_data.flatMap(lambda element: element.split(" "))
Helper.printRdd(1, 'rdd_split', rdd_split)

rdd_map = rdd_split.map(lambda element: (element, 1))
Helper.printRdd(2, 'rdd_map', rdd_map)

rdd_reduce = rdd_map.reduceByKey(lambda x,y : x+y)
Helper.printRdd(3, 'rdd_reduce', rdd_reduce)

rdd_filter = rdd_reduce.filter(lambda element: 'r' in element[0])
Helper.printRdd(4, 'rdd_filter', rdd_filter)

myData = 'resilient distributed datasets'
data = spark.sparkContext.parallelize(myData, 2)
Helper.printRdd(5, 'data', data)

distinctOut = data.distinct() 
Helper.printRdd('6.0','distinctOut', distinctOut)
Helper.printRdd('6.1', 'distinctOutCount', distinctOut.count())

dataMap = data.map(lambda element: (element, element[0], element.startswith('d')), True)
Helper.printRdd('7.0', 'dataMap', dataMap)

dataMapFilter = dataMap.filter(lambda record: record[2]).take(5)
Helper.printRdd('7.1', 'dataMapFilter', dataMapFilter)