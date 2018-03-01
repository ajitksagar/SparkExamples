import sys
import os

# Set the path for spark installation
# this is the path where you downloaded and uncompressed the Spark download
# Using forward slashes on windows, \\ should work too.
os.environ['SPARK_HOME'] = "C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/"
# # This is the HADOOP_HOME DIR for the local mode where winutil utility is installed
os.environ['HADOOP_HOME'] = "C:/winutil/"
# # # Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append("C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/python")
# # # Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append("C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/python/lib/py4j-0.10.1-src.zip")

# try the import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf


except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
sc = SparkContext("local")
# words = sc.parallelize(["scala", "java", "hadoop", "spark", "akka"])
lines = sc.textFile("c:\spark\README.md.txt")
words = lines.flatMap(lambda line : line.split(" ")).map(lambda word:(word,1))
Wordswithoutspecialchars = lines.flatMap(lambda line : line.split(" ")).filter(lambda word : word.isalnum()).map(lambda word:(word,1))
counts = words.reduceByKey(lambda a,b : a+b)
# counts.saveAsTextFile("c:\spark\spark2\wordcount")
Wordswithoutspecialchars.saveAsTextFile("c:\spark\spark2\wordcount1")


