package com.stan.spark.sql.test

object FormatDecimal {

  def main(args: Array[String]): Unit = {
    val d1: Double = 6.66666
    val d2: Double = 8.888888

    println(d1.formatted("%.3f"))

    println(d2.formatted("%.2f"))
  }

}
