package com.nyist.sparkJobBySkm

import org.apache.spark.sql.SparkSession

/**
  * @Author: skm
  * @Date: 2019/5/15 18:26
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobFourHive {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .appName("BySkm")
      .master("local[4]")
      .config("spark.sql.warehouse.dir","hdfs://mycluster/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from sparktest.employee").show()
  }
}
