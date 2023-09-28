from pyspark import SparkContext, SparkConf

# Create a Spark configuration and SparkContext
conf = SparkConf().setAppName("breweries")
sc = SparkContext(conf=conf)

# Load a text file from HDFS (or your preferred data source)
brewfile = sc.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")

df = sc.write.csv("hdfs://namenode:9000/data/openbeer/breweries/testbreweries.csv")

sc.stop()

