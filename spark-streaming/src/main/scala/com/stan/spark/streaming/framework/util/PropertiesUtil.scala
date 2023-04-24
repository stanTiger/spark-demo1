package com.stan.spark.streaming.framework.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()
    prop.load(
      new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
        "UTF-8"))
    prop
  }
}
