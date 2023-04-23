package com.stan.spark.sql.connect

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object MySQLConnect {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MySQLConnect")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 加载数据
    // 方式一
    val accountDF: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/stan?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "abc123")
      .option("dbtable", "account")
      .load()

    //方式 2:通用的 load 方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://linux1:3306/spark-sql?user=root&password=123123",
        "dbtable" -> "user", "driver" -> "com.mysql.jdbc.Driver"))
      .load().show
    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://linux1:3306/spark-sql",
      "user", props)
    df.show

    accountDF.show()

    // 关闭资源
    spark.stop()
  }

}
