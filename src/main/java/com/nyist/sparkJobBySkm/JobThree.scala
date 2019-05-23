package com.nyist.sparkJobBySkm

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: skm
  * @Date: 2019/5/12 21:17
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobThree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BySkm")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val dataRdd = sc.textFile("C:\\Users\\21188\\Desktop\\spark实验指导书\\score.txt")

    val JimRdd = dataRdd.map(_.split(",")).map(x => (x(0), x(2).toInt)).filter(x => x._1.contains("Jim"))

    val sumRdd = JimRdd.reduceByKey(_ + _)
    //得到Jim同学的总成绩
    sumRdd.foreach(print)

    val avgRdd = JimRdd.map(x => (x._1, (x._2, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(a => (a._1, (a._2._1 / a._2._2)))
    //得到Jim同学的平均成绩，除了成绩低其他的也没什么了
    avgRdd.foreach(print)

    val scoreRdd = dataRdd.map(_.split(",")).map(x => (x(1), x(2).toInt))
      .map(x => (x._1, (x._2, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(a => (a._1, (a._2._1 / a._2._2)))

    //各门课程的平均分
    scoreRdd.foreach(println)

  }

}
