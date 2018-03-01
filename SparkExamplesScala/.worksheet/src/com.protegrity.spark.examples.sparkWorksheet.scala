package com.protegrity.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object sparkWorksheet {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(178); 
  println("Welcome to the Scala worksheet");$skip(101); 
  
  
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("worksheetTest"));System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(132); 
                                                   
 val rdd = sc.parallelize(List((1,"ajit"),(2,"shyam"),(3,"Rahul"),(4,"Vinni")));System.out.println("""rdd  : org.apache.spark.rdd.RDD[(Int, String)] = """ + $show(rdd ));$skip(22); val res$0 = 
  
  rdd.groupByKey();System.out.println("""res0: org.apache.spark.rdd.RDD[(Int, Iterable[String])] = """ + $show(res$0))}
 
 
  
}
