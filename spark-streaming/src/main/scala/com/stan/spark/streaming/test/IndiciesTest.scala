package com.stan.spark.streaming.test

object IndiciesTest {

  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(1, 2, 3)
    for (i <- arr.indices) {
      println(i)
    }
  }

}
