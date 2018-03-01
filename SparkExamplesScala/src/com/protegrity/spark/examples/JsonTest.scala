package com.protegrity.spark.examples

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object JsonTest {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    val sc = new SparkContext(new SparkConf().setAppName("SmokeTest").setMaster("local[*]"))

    sc.setLogLevel("ERROR")
    
    val sqlContext = new SQLContext(sc)
    
    val schema: StructType = new StructType().add("id",IntegerType).add("first_name",StringType) 

                            
    val dataframe = sqlContext.read.schema(schema).json("resources/nulljsontest.json")
    
    dataframe.registerTempTable("dfTable")
    
    dataframe.printSchema()
    
    dataframe.show
    
/*    
    sqlContext.udf.register("ptyProtectInt", com.protegrity.spark.udf.ptyProtectInt _)
    sqlContext.udf.register("ptyProtectStr", com.protegrity.spark.udf.ptyProtectStr _)
    sqlContext.udf.register("ptyUnprotectInt", com.protegrity.spark.udf.ptyUnprotectInt _)
    sqlContext.udf.register("ptyUnprotectStr", com.protegrity.spark.udf.ptyUnprotectStr _)*/
    
    val protectDF = sqlContext.sql("select ptyProtectInt(id,'TE_INT_4') as id,ptyProtectStr(first_name,'TE_A_S13_L0R0_Y') as name from dfTable") 
    
    protectDF.show
    
    protectDF.registerTempTable("proTable")
    
    val unprotectDF = sqlContext.sql("select ptyUnprotectInt(id,'TE_INT_4') as id,ptyUnprotectStr(name,'TE_A_S13_L0R0_Y') as name from proTable")
    
    unprotectDF.show
    
  }

}