package com.protegrity.spark.examples


object test {
 
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val ages = List(22,18,45,76,10,15)              //> ages  : List[Int] = List(22, 18, 45, 76, 10, 15)
  
  val grouped = ages.groupBy { age =>
  					if (age >= 18 && age <= 45) "Adult"
  					else if ( age < 18) "Child"
  					else "senior"
  }                                               //> grouped  : scala.collection.immutable.Map[String,List[Int]] = Map(Child -> L
                                                  //| ist(10, 15), Adult -> List(22, 18, 45), senior -> List(76))
 
 val name = "ajit"                                //> name  : String = ajit
 
 println(name)                                    //> ajit

def readValue[T](x: String): T = {

x.asInstanceOf[T]

}                                                 //> readValue: [T](x: String)T

readValue[Int]("1.0")                             //> java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.I
                                                  //| nteger
                                                  //| 	at scala.runtime.BoxesRunTime.unboxToInt(BoxesRunTime.java:101)
                                                  //| 	at com.protegrity.spark.examples.test$$anonfun$main$1.apply$mcV$sp(com.p
                                                  //| rotegrity.spark.examples.test.scala:26)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$$anonfun$$exe
                                                  //| cute$1.apply$mcV$sp(WorksheetSupport.scala:76)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.redirected(W
                                                  //| orksheetSupport.scala:65)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.$execute(Wor
                                                  //| ksheetSupport.scala:75)
                                                  //| 	at com.protegrity.spark.examples.test$.main(com.protegrity.spark.example
                                                  //| s.test.scala:4)
                                                  //| 	at com.protegrity.spark.examples.test.main(com.protegrity.spark.examples
                                                  //| .test.scala)
 
}