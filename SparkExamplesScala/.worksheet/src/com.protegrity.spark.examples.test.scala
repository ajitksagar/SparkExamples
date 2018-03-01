package com.protegrity.spark.examples


object test {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(99); 
 
  println("Welcome to the Scala worksheet");$skip(40); 
  
  val ages = List(22,18,45,76,10,15);System.out.println("""ages  : List[Int] = """ + $show(ages ));$skip(144); 
  
  val grouped = ages.groupBy { age =>
  					if (age >= 18 && age <= 45) "Adult"
  					else if ( age < 18) "Child"
  					else "senior"
  };System.out.println("""grouped  : scala.collection.immutable.Map[String,List[Int]] = """ + $show(grouped ));$skip(21); 
 
 val name = "ajit";System.out.println("""name  : String = """ + $show(name ));$skip(17); 
 
 println(name);$skip(58); 

def readValue[T](x: String): T = {

x.asInstanceOf[T]

};System.out.println("""readValue: [T](x: String)T""");$skip(23); val res$0 = 

readValue[Int]("1.0");System.out.println("""res0: Int = """ + $show(res$0))}
 
}
