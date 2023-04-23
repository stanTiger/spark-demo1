package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByOperator {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 算子
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"))
    // 按首字母分组
    //    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.toCharArray()(0))
    // wordCount
    val wordAndIter: RDD[(String, Iterable[String])] = rdd.groupBy(word => word)
    val word2Count: RDD[(String, Int)] = wordAndIter.mapValues(iter => iter.count(elem => true))


    word2Count.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
