package com.nyist.sparkJobBySkm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: skm
  * @Date: 2019/5/22 10:19
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobFiveBroadCast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("BySkm")

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val data = sc.textFile("C:\\Users\\21188\\Desktop\\spark实验指导书\\movies.dat")
    val data1 = sc.textFile("C:\\Users\\21188\\Desktop\\spark实验指导书\\ratings.dat")

    val result1 = data.map(_.split("::")).map(x => (x(0), x(1)))
    val result2 = data1.map(_.split("::")).map(x => (x(1), x(2)))

    val result = result1.join(result2)
    val res = result.map(x => (x._2._1, (x._2._2.toDouble, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(d => (d._1, (d._2._1 / d._2._2)
        .formatted("%.2f")))

    val bc = sc.broadcast(4.0)
  //过滤评分大于4.0的电影
    val s = res.map(x=>(x._1,x._2.toDouble>bc.value)).filter(_._2==true)

    s.foreach(println)



  }
}
