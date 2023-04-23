package com.stan.spark.sql.udf

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


class MyAverageUDAF extends Aggregator[Int, (Long, Long), Double] {
  override def zero: (Long, Long) = (0l, 0l)

  override def reduce(b: (Long, Long), a: Int): (Long, Long) = (b._1 + a, b._2 + 1)

  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = (b1._1 + b2._1, b1._2 + b2._2)

  override def finish(reduction: (Long, Long)): Double = reduction._1.toDouble / reduction._2.toDouble

  override def bufferEncoder: Encoder[(Long, Long)] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
