package com.protegrity.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wc {
  
  def main(args: Array[String]): Unit = {
    
    
  
  val sc = new  SparkContext(new SparkConf().setAppName("WordCount"))
  
  sc.setLogLevel("ERROR")
  
  val rdd = sc.textFile(args(0))
  
     
  val words = rdd.flatMap(x => x.split(" "))
  
  val wordcount = words.map(word => (word,1)).reduceByKey(_ + _)
    
  wordcount.saveAsTextFile(args(1))
    
  }
  
  
}