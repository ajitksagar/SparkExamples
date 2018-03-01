import sys
import os

# Set the path for spark installation
# this is the path where you downloaded and uncompressed the Spark download
# Using forward slashes on windows, \\ should work too.
os.environ['SPARK_HOME'] = "C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/"
# # # # This is the HADOOP_HOME DIR for the local mode where winutil utility is installed
os.environ['HADOOP_HOME'] = "C:/winutil/"
# # # # # Append the python dir to PYTHONPATH so that pyspark could be found
# sys.path.append("C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/python")
# # # # # Append the python/build to PYTHONPATH so that py4j could be found
# sys.path.append("C:/spark/spark2/spark-2.0.0-bin-hadoop2.7/python/lib/py4j-0.10.1-src.zip")

# try the import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
sc = SparkContext("local[*]")


RDD1 = sc.parallelize(["coffee","monkey","kitty","ajit","coffee"])
RDD2 = sc.parallelize(["coffee","panda","tea","coke"])

# Distinct transformation on RDD to produce new RDD with distinct elements.
distinctRDD = RDD1.distinct()
distinctCollection = distinctRDD.collect()

print "Distinct RDD is :",distinctCollection

# Union Transformation which Joins the two RDDs.
unionRDD = RDD1.union(RDD2)
unionCollection = unionRDD.collect()

print "Union RDD -> RDD1 U RDD2 is :",unionCollection

# Intersection Transformation which returns elements which are common between two RDDs.

intersectionRDD = RDD1.intersection(RDD2)
intersectionCollection = intersectionRDD.collect()

print "Intersect RDD -> RDD1 ~ RDD2 is :", intersectionCollection

# Substract Transformation takes in another RDD and returns an RDD that only has
# values present in the first RDD and not the second RDD.

substractRDD = RDD1.subtract(RDD2)
substractCollection = substractRDD.collect()

print "Substract RDD -> RDD1 - RDD2 is :",substractCollection

# The cartesian(other) transformation results in possible pairs of (a, b) where a is in the source RDD and
# b is in the other RDD.

userRDD = sc.parallelize(["user1","user2","user3"])
interestRDD = sc.parallelize(["tea","coffee","coke","beer"])

cartesianRDD = userRDD.cartesian(interestRDD)
cartesianCollection = cartesianRDD.collect()

print "Cartesian RDD -> RDD1 * RDD2 is:", cartesianCollection

