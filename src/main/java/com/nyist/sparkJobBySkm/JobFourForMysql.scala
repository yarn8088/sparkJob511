package com.nyist.sparkJobBySkm

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * @Author: skm
  * @Date: 2019/5/13 21:35
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobFourForMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("BySkm")
      .getOrCreate()

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "hadoop")

    val url = "jdbc:mysql://master:3306/sparktest"
    val table = "employee"

    val mysqlDf = spark.read.jdbc(url, table, prop)

    //    mysqlDf.show()

    //第二种方式
    val mysqDf2 = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://master:3306/sparktest")
      .option("dbtable", "employee")
      .option("user", "root")
      .option("password", "hadoop")
      .load()

    //   mysqDf2.show()

    //将df中的数据插入到mysql中
    val dataFlocal = spark.read.json("C:\\Users\\21188\\Desktop\\spark实验指导书\\employee.json")
    //   dataFlocal.show()
       dataFlocal.write.mode("append").jdbc(url,table,prop)

  }

}
