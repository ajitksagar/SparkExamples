package com.protegrity.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    
  System.setProperty("hadoop.home.dir", "c:\\winutil\\");
  
  val sc = new  SparkContext(new SparkConf().setAppName("WordCount").setMaster("local[*]"))
  
  val appId = sc.applicationId
  
  sc.setLogLevel("ERROR")
  
  val str = "This is not fair at all, So tell me what needs to be done is is is"
  
  val rdd = sc.parallelize(Seq(str))
  
  println("String RDD ******************")
  rdd.collect().foreach(println)
  
    
  val words = rdd.flatMap(x => x.split(" "))
  
    println("words RDD ******************")
  
    words.collect().foreach(println)
  
    
    println("word count RDD ******************")
  
    val wordcount = words.map(word => (word,1)).reduceByKey(_ + _)
    
  wordcount.foreach(println)
  
  println("Aplication ID *********************************"+appId)
    
  }
  
  
}