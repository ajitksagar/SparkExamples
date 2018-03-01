package com.protegrity.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object reduceByKeyVsgroupByKey {
  
  // Find total trips and price for customer
   
   case class customer(id: String,destination: String,price: Double)
  
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
     
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("reduceByKeyVsgroupByKey"))
    
    sc.setLogLevel("ERROR")    
    
    val purchaseRDD = sc.parallelize(List(customer("123","Paris",23.04),customer("456","London",45.06),customer("789","New York",88.07),
                                      customer("123","Mumbai",25.76),customer("456","Delhi",75.06),customer("789","Amsterdam",88.07)))
                                      
                                         
  val groupByKeyRDD = purchaseRDD.map(p => (p.id,p.price)).groupByKey().mapValues( p =>(p.size,p.sum))
                                      
  val reduceByKeyRDD = purchaseRDD.map (p => (p.id,(1,p.price))).reduceByKey((v1,v2) => ((v1._1 + v2._1),(v1._2 + v2._2)))
    
    println("*********** groupByKey**********")  
  
    groupByKeyRDD.collect().foreach(println)
    
    println("*********** reduceByKey**********")
    
    reduceByKeyRDD.collect().foreach(println)
    
   }
}