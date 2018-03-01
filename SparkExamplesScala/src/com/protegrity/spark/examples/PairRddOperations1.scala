package com.protegrity.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


case class Event(organizer: String, name: String,budget: Int)


 /**
 * Definitions of Transformations:
 * 
 * def groupByKey(): RDD[(K,Iterable[V])]
 * def reduceByKey(func: (v,v) => V): RDD[(K,V)]
 * def countByKey(): Map[K,Long]
 * 
 * */

object PairRddOperations1 {
  
  def main(args: Array[String]) {
    
    
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    
    val sc = new SparkContext(new SparkConf().setAppName("groupByKeyTransformation").setMaster("local[*]"))
    
    sc.setLogLevel("ERROR")
    
    val events = List(Event("Bruce","Wayne Corp.",6000),Event("Krishi","Google Inc.",8000),Event("Tony","Stark Inc.",7000),
        Event("Tony","EMC Inc.",7500),Event("Bruce","Dell Inc.",9500),Event("Tony","Apple Inc.",6500))
        
    val eventsRDD = sc.parallelize(events, 2)
    
    val eventsPairRDD = eventsRDD.map(event => (event.organizer,event.budget))
    
    
    println("****************** groupByKey() Transformation *****************")
    val groupedRDD = eventsPairRDD.groupByKey()
    
    groupedRDD.collect().foreach(println)
    
    /** 
     * After performing action on the groupedRDD 
     * it will return following result after grouping by the organizer
     * (Tony,CompactBuffer(7000, 7500, 6500))
		 * (Ajit,CompactBuffer(6000, 9500))
		 * (Krishi,CompactBuffer(8000))
     */
    
    println("****************** reduceByKey() Transformation *****************")
    val budgetRDD = eventsPairRDD.reduceByKey(_ + _)
    
    
    budgetRDD.collect().foreach(println)
    
    /**
     *  After calling action on budgetRDD 
     *  it will return the following result after reducing by the given (_ + _) function 
     *  (Tony,21000)
     *  (Ajit,15500)
		 *	(Krishi,8000)
     */
    
    println("****************** countByKey() Action *****************")
        
    eventsPairRDD.countByKey().foreach(println)
    
    /**
     * countByKey() is an action which returns the number of counts per key.
     */
    
    
    /**
     * Here, mapvalues maps the values per key.
     * We can also say it as shorthand of map.
     * map(case(K,V) => (K,func(V)))
     */
    
    val intermediateRDD = eventsPairRDD.mapValues { budget => (budget,1) }
                          .reduceByKey((v1,v2) => (v1._1 + v2._1,v1._2 + v2._2))
 
    println("**************************** Budget and Number of Events Per Organizer **************")
    
    intermediateRDD.collect().foreach(println)
    
    println("**************************** Avg. Budget Per Organizer **************")
    
    val avgBudgetRDD = intermediateRDD.mapValues {
                            
                            case (totalbudget,numberofevents) => totalbudget/numberofevents
                          }
                          
    avgBudgetRDD.collect().foreach(println)
    
}
  
}