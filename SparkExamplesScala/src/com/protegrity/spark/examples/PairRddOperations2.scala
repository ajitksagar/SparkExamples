package com.protegrity.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object PairRddOperations2 {
  
  
  val subscription = List((101,("Rueteli","AG")),(102,("Brelaz", "DemiTarif")),
                          (103,("Gress","DemiTarifVisa")), (104,("Schatten","DemiTarif")))
                        
  val locations = List((101,"Bern"),(101,"Thun"),(102,"Lucane"),(102,"Geneve"),
                        (102,"Nyon"),(103,"Zurich"),(103,"St-Gallen"),(103,"Chur"),(105,"Geneve"))                       
  
  def TrackedCustomers(subs: RDD[(Int,(String,String))],location:  RDD[(Int,String)] ) = {
        
    val innerJoinRDD = subs.join(location)
    
    innerJoinRDD.collect().foreach(println)
  }
  
  def SubscribedAppUsersIncluded(subs: RDD[(Int,(String,String))],location:  RDD[(Int,String)] ) = {
        
    val leftOuterJoinRDD = subs.leftOuterJoin(location)
    
    leftOuterJoinRDD.collect().foreach(println)
  }
  
  def UnsubscribedAppUsersIncluded(subs: RDD[(Int,(String,String))],location:  RDD[(Int,String)] ) = {
        
    val rightOuterJoinRDD = subs.rightOuterJoin(location)
    
    rightOuterJoinRDD.collect().foreach(println)
  }
  
  
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    
    val sc = new SparkContext(new SparkConf().setAppName("InnerJoinExample").setMaster("local[*]"))
    
    sc.setLogLevel("ERROR")
    
    val subsRDD = sc.parallelize(subscription)
    
    val locationRDD = sc.parallelize(locations)
    
    println("********* Subscribed Customers Using App for Ticket Booking *********")
    TrackedCustomers(subsRDD, locationRDD)
    
    println("********* Subscribed Customers May/not be Using App for Ticket Booking *********")
    SubscribedAppUsersIncluded(subsRDD, locationRDD)
    
    println("********* Subscribed/Unsubscribed Customers Using App for Ticket Booking *********")
    UnsubscribedAppUsersIncluded(subsRDD, locationRDD)
    
  }
  
  
}