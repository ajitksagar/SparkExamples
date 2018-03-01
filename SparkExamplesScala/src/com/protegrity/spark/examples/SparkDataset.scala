package com.protegrity.spark.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkDataset {
 
  
  def main(args: Array[String]) = {
    
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    
    val sc = new SparkContext(new SparkConf().setAppName("Dataset").setMaster("local[*]"))
    
    sc.setLogLevel("ERROR")
    
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    val linesRDD = sc.textFile("input/data.txt")
    
    val wordsRDD = linesRDD.flatMap(_.split(" ")).filter(_.equalsIgnoreCase("spark"))
    
      
    val linesDataframe = sqlContext.read.text("input/data.txt")
    
    println("Dataframe Output **************")
    
    linesDataframe.as("SparkWordsDataframe").show(2,false)
           
    val DataframeToRDD = linesDataframe.rdd.map(row => row.toString())
                         .flatMap(_.split(" ")).filter(_.equalsIgnoreCase("spark"))
    
    println("Dataframe Converted to RDD Output **************")                                                  
                         
    DataframeToRDD.collect().foreach(println)
    
    println("Spark word count through RDD: " +DataframeToRDD.count())
    
    val linesDataset = sqlContext.read.text("input/data.txt").as[String]
    
    linesDataset.show(2,false)
    val sparkWords= linesDataset.flatMap(row =>  {
      
    val regex =""      
      row.split(regex)}).filter(_.equalsIgnoreCase("spark"))
    
    println("Dataset Output **************")
    
    sparkWords.as("SparkWordsDataset").show(false)
    
    println("Spark word count through Dataset: " +sparkWords.count())
    
  }
}