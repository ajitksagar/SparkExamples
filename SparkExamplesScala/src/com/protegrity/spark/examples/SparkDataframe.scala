package com.protegrity.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


case class Employee(id: Int, name: String, salary: Long)

object SparkDataframe {
  
  
  def main(args: Array[String]): Unit = {
    
    
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    
    val sc = new SparkContext(new SparkConf().setAppName("DataFrameExample").setMaster("local[*]"))
    
    sc.setLogLevel("ERROR")
    
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    import org.apache.spark.sql.functions
    
    val employeeRDD = sc.parallelize(Seq(Employee(95210,"Bruce",658424),Employee(95211,"Alex",758420),Employee(95215,"Lynn",858420),Employee(95211,"Jennifer",828420)))
    
    employeeRDD.collect().foreach(println)
    
    val employeeDFImplicit = employeeRDD.toDF()
    
    employeeDFImplicit.show()
    
    val employeeDF = sc.textFile("input/employee.txt").map(_.split(",")).map(emp => Employee(emp(0).toInt,emp(1),emp(2).toLong)).toDF()
    
    
    employeeDF.show()
    
    val employeeDS = employeeRDD.toDS()
    
    val DS1 = employeeDF.as[Employee]
    
    employeeDS.show()
    
    val empNormalizeSalary = DS1.filter( emp => emp.salary < 800000).map(emp => emp.salary*0.10 + emp.salary).as("NormalisedSalary")
    
    empNormalizeSalary.show()
    
  }
  
}