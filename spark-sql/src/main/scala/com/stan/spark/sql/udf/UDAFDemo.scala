package com.stan.spark.sql.udf

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

object UDAFDemo {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDAF")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 加载数据
    val userRDD: RDD[(String, Int)] = spark.sparkContext.makeRDD((List(("zhangsan", 20), ("lisi", 30), ("wangw", 40))))
    import spark.implicits._
    val userDs: Dataset[User] = userRDD.map {
      case (name, age) => User(name, age)
    }
      .toDS()

    //    val avgAge: MyAverageUDAF = new MyAverageUDAF
    //    // 注册函数
    //    spark.udf.register("avgAge", functions.udaf(avgAge))
    val udaf: MyUDAF = new MyUDAF
    spark.udf.register("avgAge",udaf)

    // 使用函数
    userDs.createTempView("user")
    spark.sql("select avgAge(age) from user").show()

    // 关闭资源
    spark.stop()
  }

}


case class User(name: String, age: Int)