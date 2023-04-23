package com.stan.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_Demo1 {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 引入隐式
    import spark.implicits._
    // 读取数据
    val df: DataFrame = spark.read.json("input/user.json")
    // 创建临时表功后续使用
    df.createTempView("user")
    // 查询 user 表中数据
    spark.sql("select avg(age) from user").show()

    // rdd
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))
    // rdd to df
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    df1.show()
    // df to ds
    val ds: Dataset[User] = df1.as[User]
    ds.show()

    ds.createTempView("user2")
    println("=====================")
    spark.sql("select id,name from user2").show()

    //ds to df
    val df2: DataFrame = ds.toDF()
    //df to rdd
    val rdd1: RDD[Row] = df2.rdd
    println("-----------------df to rdd-------------------")
    rdd1.foreach(
      line => println(line.getString(1))
    )

    // rdd => dataset
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }
      .toDS()

    println("------ rdd => dataset -------------")
    ds2.show()


    // 关闭资源
    spark.stop()

  }

}

case class User(id: Int, name: String, age: Int)
