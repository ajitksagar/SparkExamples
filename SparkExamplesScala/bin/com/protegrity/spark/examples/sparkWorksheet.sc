package com.protegrity.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object sparkWorksheet {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("worksheetTest"))
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 17/09/19 15:57:17 INFO SparkContext: Running Spark version 2.1.0
                                                  //| 17/09/19 15:57:18 ERROR Shell: Failed to locate the winutils binary in the h
                                                  //| adoop binary path
                                                  //| java.io.IOException: Could not locate executable null\bin\winutils.exe in th
                                                  //| e Hadoop binaries.
                                                  //| 	at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:379)
                                                  //| 	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:394)
                                                  //| 	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:387)
                                                  //| 	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:80)
                                                  //| 	at org.apache.hadoop.security.SecurityUtil.getAuthenticationMethod(Secur
                                                  //| ityUtil.java:611)
                                                  //| 	at org.apache.hadoop.security.UserGroupInformation.initialize(UserGroupI
                                                  //| nformation.java:273)
                                                  //| 	at org.apache.hadoop.security.UserGroupInformation.ensureInitialized(Use
                                                  //| rGroupInformation.java:261)
                                                  //| 	at org.apache.hadoop.security
                                                  //| Output exceeds cutoff limit.
                                                   
 val rdd = sc.parallelize(List((1,"ajit"),(2,"shyam"),(3,"Rahul"),(4,"Vinni")))
                                                  //> rdd  : org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[0] at
                                                  //|  parallelize at com.protegrity.spark.examples.sparkWorksheet.scala:12
  
  rdd.groupByKey()                                //> res0: org.apache.spark.rdd.RDD[(Int, Iterable[String])] = ShuffledRDD[1] at 
                                                  //| groupByKey at com.protegrity.spark.examples.sparkWorksheet.scala:14
 
 
  
}