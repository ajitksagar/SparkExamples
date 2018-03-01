package com.protegrity.spark.examples

object ScalaTutorial {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val a = 5                                       //> a  : Int = 5
  val b = 6                                       //> b  : Int = 6
  
  def sum(x: Int,y: Int):Int =  { x+y}            //> sum: (x: Int, y: Int)Int
  
  println(sum(a,b))                               //> 11
 
  
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