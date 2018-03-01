package com.protegrity.spark.examples

import java.sql.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SaveMode
import scala.math.BigDecimal
import org.apache.spark.sql.functions.unix_timestamp


/**
 * This test will test all the UDFs for protect,unprotect,reprotect,unprotectafterreprotect
 * 
 * Following input parameters will be required:
 * 
 * 0 DataElementFilePath
 * 1 InputFilePath
 * 2 ProtectOutputFilePath
 * 3 UnprotectOutputFilePath
 * 4 ReprotectOutputFilePath
 * 5 UnprotectAfterReprotectOutputFilePath
 * 6 CLearDataForvalidation
 * 
 */
 
object SqlSmokeTest {

  case class Employee(shortCol: Short, intCol: Int, stringCol: String, longCol: Long, floatCol: Float, doubleCol: Double, decimalCol: BigDecimal, dateCol: String, datetimeCol: String, unicodeCol: String)

  case class Dataelements(shortDE: String, intDE: String, stringDE: String, longDE: String, floatDE: String, doubleDE: String, decimalDE: String, dateDE: String, datetimeDE: String, unicodeDE: String)

  def main(args: Array[String]): Unit = {

    /*//Configurations to run the Spark Job locally
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    
    val sc = new SparkContext(new SparkConf().setAppName("SmokeTest").setMaster("local[*]"))*/

    import scala.io.Source

    // Reading dataelements for Protect operation
    val protectDataelement = Source.fromFile(args(0)).getLines.map(line => line.split(",")).map(de => Dataelements(de(0), de(1), de(2), de(3), de(4), de(5), de(6), de(7), de(8), de(9))).toList

    
    val sc = new SparkContext(new SparkConf().setAppName("SmokeTest"))
    
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val rdd = sc.textFile(args(1))

    
    // Reading dataelements for Reprotect operation
//    val reprotectDataelement = Source.fromFile("resources/dataelementfile").getLines.map(line => line.split(",")).map(de => Dataelements(de(0), de(1), de(2), de(3), de(4), de(5), de(6), de(7), de(8), de(9))).next()

    import org.apache.spark.sql.functions

    val employeeDF = rdd.map(line => line.split(",")).map(emp => Employee(emp(0).toShort, emp(1).toInt, emp(2), emp(3).toLong, emp(4).toFloat, emp(5).toDouble, BigDecimal(emp(6)), emp(7), emp(8), emp(9))).toDF()

    employeeDF.show(false)

    println("After creating temp table")

    employeeDF.registerTempTable("Employee")

    val clearDF = sqlContext.sql("select * from Employee")

    clearDF.show()

    clearDF.rdd.map(_.mkString(",")).saveAsTextFile(args(6))
    
    //UDFs for Protect Operation
    sqlContext.udf.register("ptyProtectShort", com.protegrity.spark.udf.ptyProtectShort _)
    sqlContext.udf.register("ptyProtectInt", com.protegrity.spark.udf.ptyProtectInt _)
    sqlContext.udf.register("ptyProtectLong", com.protegrity.spark.udf.ptyProtectLong _)
    sqlContext.udf.register("ptyProtectFloat", com.protegrity.spark.udf.ptyProtectFloat _)
    sqlContext.udf.register("ptyProtectDouble", com.protegrity.spark.udf.ptyProtectDouble _)
    sqlContext.udf.register("ptyProtectDecimal", com.protegrity.spark.udf.ptyProtectDecimal _)
    sqlContext.udf.register("ptyProtectStr", com.protegrity.spark.udf.ptyProtectStr _)
    sqlContext.udf.register("ptyProtectUnicode", com.protegrity.spark.udf.ptyProtectUnicode _)
    sqlContext.udf.register("ptyProtectDate", com.protegrity.spark.udf.ptyProtectDate _)
    sqlContext.udf.register("ptyProtectDateTime", com.protegrity.spark.udf.ptyProtectDateTime _)

    //UDFs for Unprotect Operation
    sqlContext.udf.register("ptyUnprotectShort", com.protegrity.spark.udf.ptyUnprotectShort _)
    sqlContext.udf.register("ptyUnprotectInt", com.protegrity.spark.udf.ptyUnprotectInt _)
    sqlContext.udf.register("ptyUnprotectLong", com.protegrity.spark.udf.ptyUnprotectLong _)
    sqlContext.udf.register("ptyUnprotectFloat", com.protegrity.spark.udf.ptyUnprotectFloat _)
    sqlContext.udf.register("ptyUnprotectDouble", com.protegrity.spark.udf.ptyUnprotectDouble _)
    sqlContext.udf.register("ptyUnprotectDecimal", com.protegrity.spark.udf.ptyUnprotectDecimal _)
    sqlContext.udf.register("ptyUnprotectStr", com.protegrity.spark.udf.ptyUnprotectStr _)
    sqlContext.udf.register("ptyUnprotectUnicode", com.protegrity.spark.udf.ptyUnprotectUnicode _)
    sqlContext.udf.register("ptyUnprotectDate", com.protegrity.spark.udf.ptyUnprotectDate _)
    sqlContext.udf.register("ptyUnprotectDateTime", com.protegrity.spark.udf.ptyUnprotectDateTime _)

    //UDFs for Reprotect Operation
    sqlContext.udf.register("ptyReprotectShort", com.protegrity.spark.udf.ptyReprotectShort _)
    sqlContext.udf.register("ptyReprotectInt", com.protegrity.spark.udf.ptyReprotectInt _)
    sqlContext.udf.register("ptyReprotectLong", com.protegrity.spark.udf.ptyReprotectLong _)
    sqlContext.udf.register("ptyReprotectFloat", com.protegrity.spark.udf.ptyReprotectFloat _)
    sqlContext.udf.register("ptyReprotectDouble", com.protegrity.spark.udf.ptyReprotectDouble _)
    sqlContext.udf.register("ptyReprotectDecimal", com.protegrity.spark.udf.ptyReprotectDecimal _)
    sqlContext.udf.register("ptyReprotectStr", com.protegrity.spark.udf.ptyReprotectStr _)
    sqlContext.udf.register("ptyReprotectUnicode", com.protegrity.spark.udf.ptyReprotectUnicode _)
    sqlContext.udf.register("ptyReprotectDate", com.protegrity.spark.udf.ptyReprotectDate _)
    sqlContext.udf.register("ptyReprotectDateTime", com.protegrity.spark.udf.ptyReprotectDateTime _)

//    val employeeProtectDF = sqlContext.sql(s"Select ptyProtectShort(shortCol, '${protectDataelement(0).shortDE}') as shortCol,ptyProtectInt(intCol, '${protectDataelement(0).intDE}') as intCol,ptyProtectStr(stringCol,'${protectDataelement(0).stringDE}') as stringCol, ptyProtectLong(longCol,'${protectDataelement(0).longDE}') as longCol ,ptyProtectFloat(floatCol,'${protectDataelement(0).floatDE}') as floatCol,ptyProtectDouble(doubleCol,'${protectDataelement(0).doubleDE}') as doubleCol,ptyProtectDecimal(decimalCol,'${protectDataelement(0).decimalDE}') as decimalCol,ptyProtectDate(dateCol,'${protectDataelement(0).dateDE}') as dateCol,ptyProtectDateTime(datetimeCol,'${protectDataelement(0).datetimeDE}') as datetimeCol,ptyProtectUnicode(unicodeCol,'${protectDataelement(0).unicodeDE}') as unicodeCol from Employee")

     val employeeProtectDF = sqlContext.sql(s"Select ptyProtectShort(shortCol, '${protectDataelement(0).shortDE}') as shortCol,ptyProtectInt(intCol, '${protectDataelement(0).intDE}') as intCol,ptyProtectStr(stringCol,'${protectDataelement(0).stringDE}') as stringCol, ptyProtectLong(longCol,'${protectDataelement(0).longDE}') as longCol ,ptyProtectFloat(floatCol,'${protectDataelement(0).floatDE}') as floatCol,ptyProtectDouble(doubleCol,'${protectDataelement(0).doubleDE}') as doubleCol,ptyProtectDecimal(decimalCol,'${protectDataelement(0).decimalDE}') as decimalCol,ptyProtectDate(dateCol,'${protectDataelement(0).dateDE}') as dateCol,ptyProtectDateTime(datetimeCol,'${protectDataelement(0).datetimeDE}') as datetimeCol,ptyProtectUnicode(unicodeCol,'${protectDataelement(0).unicodeDE}') as unicodeCol from Employee")
    
    employeeProtectDF.registerTempTable("EmployeeProtected")

    // Writing protected data
//    employeeProtectDF.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(args(2))
    
    employeeProtectDF.rdd.map(_.mkString(",")).saveAsTextFile(args(2))

    val employeeUnprotectDF = sqlContext.sql(s"Select ptyUnprotectShort(shortCol, '${protectDataelement(0).shortDE}') as shortCol,ptyUnprotectInt(intCol, '${protectDataelement(0).intDE}') as intCol,ptyUnprotectStr(stringCol,'${protectDataelement(0).stringDE}') as stringCol, ptyUnprotectLong(longCol,'${protectDataelement(0).longDE}') as longCol ,ptyUnprotectFloat(floatCol,'${protectDataelement(0).floatDE}') as floatCol,ptyUnprotectDouble(doubleCol,'${protectDataelement(0).doubleDE}') as doubleCol,ptyUnprotectDecimal(decimalCol,'${protectDataelement(0).decimalDE}') as decimalCol,ptyUnprotectDate(dateCol,'${protectDataelement(0).dateDE}') as dateCol,ptyUnprotectDateTime(datetimeCol,'${protectDataelement(0).datetimeDE}') as datetimeCol,ptyUnprotectUnicode(unicodeCol,'${protectDataelement(0).unicodeDE}') as unicodeCol from EmployeeProtected")

    // Writing unprotected data
//    employeeUnprotectDF.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(args(3))
    
    employeeUnprotectDF.rdd.map(_.mkString(",")).saveAsTextFile(args(3))

    val employeeReprotectDF = sqlContext.sql(s"Select ptyReprotectShort(shortCol, '${protectDataelement(0).shortDE}','${protectDataelement(1).shortDE}') as shortCol,ptyReprotectInt(intCol, '${protectDataelement(0).intDE}','${protectDataelement(1).intDE}') as intCol,ptyReprotectStr(stringCol,'${protectDataelement(0).stringDE}','${protectDataelement(1).stringDE}') as stringCol, ptyReprotectLong(longCol,'${protectDataelement(0).longDE}','${protectDataelement(1).longDE}') as longCol ,ptyReprotectFloat(floatCol,'${protectDataelement(0).floatDE}','${protectDataelement(1).floatDE}') as floatCol,ptyReprotectDouble(doubleCol,'${protectDataelement(0).doubleDE}','${protectDataelement(1).doubleDE}') as doubleCol,ptyReprotectDecimal(decimalCol,'${protectDataelement(0).decimalDE}','${protectDataelement(1).decimalDE}') as decimalCol,ptyReprotectDate(dateCol,'${protectDataelement(0).dateDE}','${protectDataelement(1).dateDE}') as dateCol,ptyReprotectDateTime(datetimeCol,'${protectDataelement(0).datetimeDE}','${protectDataelement(1).datetimeDE}') as datetimeCol,ptyReprotectUnicode(unicodeCol,'${protectDataelement(0).unicodeDE}','${protectDataelement(1).unicodeDE}') as unicodeCol from EmployeeProtected")

    employeeReprotectDF.registerTempTable("EmployeeReprotected")

    // Writing protected data
//    employeeReprotectDF.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(args(4))
    
    employeeReprotectDF.rdd.map(_.mkString(",")).saveAsTextFile(args(4))

    val employeeUnprotectAfterReprotectDF = sqlContext.sql(s"Select ptyUnprotectShort(shortCol, '${protectDataelement(1).shortDE}') as shortCol,ptyUnprotectInt(intCol, '${protectDataelement(1).intDE}') as intCol,ptyUnprotectStr(stringCol,'${protectDataelement(1).stringDE}') as stringCol, ptyUnprotectLong(longCol,'${protectDataelement(1).longDE}') as longCol ,ptyUnprotectFloat(floatCol,'${protectDataelement(1).floatDE}') as floatCol,ptyUnprotectDouble(doubleCol,'${protectDataelement(1).doubleDE}') as doubleCol,ptyUnprotectDecimal(decimalCol,'${protectDataelement(1).decimalDE}') as decimalCol,ptyUnprotectDate(dateCol,'${protectDataelement(1).dateDE}') as dateCol,ptyUnprotectDateTime(datetimeCol,'${protectDataelement(1).datetimeDE}') as datetimeCol,ptyUnprotectUnicode(unicodeCol,'${protectDataelement(1).unicodeDE}') as unicodeCol from EmployeeReprotected")

    // Writing unprotected data after reprotect
//    employeeUnprotectAfterReprotectDF.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(args(5))
    
    employeeUnprotectAfterReprotectDF.rdd.map(_.mkString(",")).saveAsTextFile(args(5))

    /**
     * *
     * Printing data to console to validate in case of failures
     * **
     */

    println("********Clear DF**********")
    clearDF.show(false)

    println("********Protected DF**********")
    employeeProtectDF.show(false)

    println("********Unprotected DF**********")
    employeeUnprotectDF.show(false)

    println("********Reprotected DF**********")
    employeeReprotectDF.show()

    println("********Unprotect after Reprotect DF**********")
    employeeUnprotectAfterReprotectDF.show()

    /**
     * *
     * Validation of Unprotected data After Protect and Reprotect
     * *
     */

    val subtractUnprotectAfterProtectDF = clearDF.except(employeeUnprotectDF)

    println("********* Substracted Dataframe for Unprotect After Protect *********")
    subtractUnprotectAfterProtectDF.show(false)

    if (subtractUnprotectAfterProtectDF.count() != 0) {
      println("****Original & Unprotected data after Protect are not same*****")
      subtractUnprotectAfterProtectDF.show(false)

    } else {
      println("****Original & Unprotected data after Protect are same*****")
    }

    val subtractUnprotectAfterReprotectDF = clearDF.except(employeeUnprotectAfterReprotectDF)

    println("********* Substracted Dataframe for Unprotect after Reprotect*********")
    subtractUnprotectAfterReprotectDF.show(false)

    if (subtractUnprotectAfterReprotectDF.count() != 0) {
      println("****Original & Unprotected data after Reprotect are not same*****")
      subtractUnprotectAfterReprotectDF.show(false)

    } else {
      println("****Original & Unprotected data after Reprotect are same*****")
    }

  }

}