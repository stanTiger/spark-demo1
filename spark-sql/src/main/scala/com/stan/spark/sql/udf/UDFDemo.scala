package com.stan.spark.sql.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDFDemo {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDF")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    // 创建临时表 user
    df.createTempView("user")
    // 注册函数
    spark.udf.register("addName",(name: String) => "name: " + name)
    // 应用UDF
    spark.sql("select addName(username),age from user").show()


    // 关闭资源
    spark.stop()
  }

}
