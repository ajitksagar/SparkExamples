package com.protegrity.spark.examples

object ScalaTutorial {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(105); 
  println("Welcome to the Scala worksheet");$skip(15); 
  
  val a = 5;System.out.println("""a  : Int = """ + $show(a ));$skip(12); 
  val b = 6;System.out.println("""b  : Int = """ + $show(b ));$skip(42); 
  
  def sum(x: Int,y: Int):Int =  { x+y};System.out.println("""sum: (x: Int, y: Int)Int""");$skip(23); 
  
  println(sum(a,b))}
 
  
}

class Complex(real: Double,imaginery: Double) {

def re() = real
def im() = imaginery
}

object ComplexNumbers {

def main(args : Array[String]) {
val c = new Complex(2.5,6.7)

println("Real part : "+c.re+"\n Imaginery part: "+c.im)

}

}
