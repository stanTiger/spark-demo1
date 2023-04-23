package com.stan.spark.sql.connect

import com.stan.spark.sql.udf.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.Properties

object MySQLConnectWrite {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MySQLConnectWrite")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 加载数据
    val userRDD: RDD[User] = spark.sparkContext.makeRDD(List(User("lisi", 20), User("zs", 30)))
    import spark.implicits._
    val ds: Dataset[User] = userRDD.toDS()

    // 通用方式 format 指定写出类型
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/stan?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "abc123")
      .option("dbtable", "user_tbl")
      .mode(SaveMode.Append)
      .save()

    //方式 2：通过 jdbc 方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://linux1:3306/spark-sql", "user", props)

    // 关闭资源
    spark.close()
  }

}
