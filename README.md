# sulekha_holtwinters

pip install sulekha_holtwinters

Author: Sulekha Aloorravi 

Package: sulekha_holtwinters 

This package is to forecast timeseries on a Spark Dataframe using Holt winters Forecasting model.

URL to test this code: https://github.com/sulekhaaloorravi-python/sulekha_holtwinters/blob/master/test_holtwinters.ipynb

Example to load existing package data:

#import package from sulekha_holtwinters.holtwinters 

import holtwinters as hw 

#Pyspark setup 

from pyspark import SparkContext 

from pyspark.sql import SQLContext 

from pyspark.sql import Row, SparkSession 

from pyspark.sql.types 

import NumericType 

sc = SparkContext.getOrCreate() 

spark = SQLContext(sc)

#Load data available within this package 

import pkg_resources 

DB_FILE = pkg_resources.resource_filename('sulekha_holtwinters', 'data')

testDF = spark.read.csv(path = DB_FILE, header = True,inferSchema = True)

