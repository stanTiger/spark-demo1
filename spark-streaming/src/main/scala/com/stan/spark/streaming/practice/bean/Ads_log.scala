package com.stan.spark.streaming.practice.bean

case class Ads_log(timestamp: Long,
                   area: String,
                   city: String,
                   userid: String,
                   adid: String)
