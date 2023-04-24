package com.stan.spark.streaming.test

import com.stan.spark.streaming.practice.util.JdbcUtil

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

object TestJDBC {

  def main(args: Array[String]): Unit = {
    val conn: Connection = JdbcUtil.getConnection
    val sql = "select name from user_tbl"
    val pstmt: PreparedStatement = conn.prepareStatement(sql)
    val resultSet: ResultSet = pstmt.executeQuery()
    val list: ListBuffer[String] = ListBuffer[String]()
    while (resultSet.next()) {
      list.append(resultSet.getString(1))
    }

    println(list.mkString(","))
    resultSet.close()
    pstmt.close()
    conn.close()

  }

}
