package com.stan.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionByOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KVOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 处理数据
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    import org.apache.spark.HashPartitioner
    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))

    rdd2.saveAsTextFile("output")

    // 关闭连接
    sc.stop()
  }

}
