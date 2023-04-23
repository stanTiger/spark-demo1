package com.stan.spark.core.test

object ScalaListTest {

  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(3,5,7,8)
    val list1: List[Int] = list.tail
    println(list1.mkString(","))

    val list2: List[Int] = list.take(list.length - 1)
    println(list2.mkString(","))

    val tuples: List[(Int, Int)] = list.zip(list1)
    println(tuples)

    println("---------")
    val iterator: Iterator[List[Int]] = list.sliding(2, 1)
    iterator.foreach(println)
  }

}
