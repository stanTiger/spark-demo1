package com.stan.spark.sql.udf

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

// buffer 不如直接使用自定义类
class CityDescUDAF extends Aggregator[String, mutable.Map[String, Long], String] {
  override def zero: mutable.Map[String, Long] = mutable.Map[String, Long]()

  override def reduce(b: mutable.Map[String, Long], a: String): mutable.Map[String, Long] = {
    b.put(a, b.getOrElse(a, 0l) + 1)
    b
  }

  override def merge(b1: mutable.Map[String, Long], b2: mutable.Map[String, Long]): mutable.Map[String, Long] = {
    for (elem <- b2) {
      val key: String = elem._1
      val value: Long = elem._2
      b1.put(key, b1.getOrElse(key, 0l) + value)
    }
    b1
  }

  override def finish(reduction: mutable.Map[String, Long]): String = {
    val top3City: List[(String, Long)] = reduction.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
    var sum = 0
    for (elem <- reduction) {
      sum += elem._2
    }
    var totalRate = 0
    val buffer: ListBuffer[String] = new ListBuffer[String]
    for (elem <- top3City) {
      val rate: Long = elem._2 * 100 / sum
      buffer.append(s"${elem._1} ${rate}%")
      totalRate += rate
    }
    if (reduction.size > 2) {
      buffer.append(s"其他 ${100 - totalRate}")
    }

    buffer.mkString(",")
  }


  override def bufferEncoder: Encoder[mutable.Map[String, Long]] = new Encoder[mutable.Map[String, Long]] {
    override def schema: StructType = StructType(Array(StructField("city", MapType(StringType, LongType))))

    override def clsTag: ClassTag[mutable.Map[String, Long]] = ???
  }

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
