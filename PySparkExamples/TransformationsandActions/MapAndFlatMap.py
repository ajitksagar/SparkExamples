import sys
import os

# Set the path for spark installation
# this is the path where you downloaded and uncompressed the Spark download
# Using forward slashes on windows, \\ should work too.
os.environ['SPARK_HOME'] = "C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/"
# # # # This is the HADOOP_HOME DIR for the local mode where winutil utility is installed
os.environ['HADOOP_HOME'] = "C:/winutil/"
# # # # # Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append("C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/python")
# # # # # Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append("C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/python/lib/py4j-0.10.1-src.zip")

# try the import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
sc = SparkContext("local")

nums = sc.parallelize([2,3,4,5])

# Map Transformation:-
# The map transformation takes in a function  and applies it to each element in the RDD
# with the result of the function being the new value of each element in the resulting RDD.
squareRDD = nums.map(lambda x : x * x)
squared = squareRDD.collect()

print "Map Transformation's Squared output for list[2,3,4,5] ->",squared

# for num in squared:
#     print "%i " % (num)

# FlatMap Transformation:-
# Instead of returning a single element, we return an iterator with our return values.
# Rather than producing an RDD of iterators, we get back an RDD which consists of the elements from all of the iterators.

lines = sc.parallelize(["Hello there, how are you doing?","Good morning","let's have some tea"])
wordsRDD = lines.flatMap(lambda line : line.split(" "))
print "The first word from wordsRDD: ",wordsRDD.first()
