package com.stan.spark.sql.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}

class MyUDAF extends UserDefinedAggregateFunction {
  // 输入数据
  override def inputSchema: StructType = StructType(Array(StructField("age", IntegerType)))

  // 缓冲数据
  override def bufferSchema: StructType = StructType(Array(StructField("sum", LongType), StructField("cnt", LongType)))

  // 输出数据类型
  override def dataType: DataType = DoubleType

  //  稳定性：对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  // 函数缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存年龄的总和
    buffer(0) = 0l
    // 存年龄的个数
    buffer(1) = 0l
  }

  // 更新缓存数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getInt(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
}
