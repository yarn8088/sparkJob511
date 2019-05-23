package com.nyist.sparkJobBySkm

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: skm
  * @Date: 2019/5/11 21:23
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object jobTwo {
  def main(args: Array[String]): Unit = {
    //配置配置信息，这里采用集群提交
    val conf = new SparkConf().setMaster("local[4]")
      .setAppName("BySKM")

    val sc = new SparkContext(conf)

    //读取数据文件加载为rdd
    val dataRdd = sc.textFile("C:\\Users\\21188\\Desktop\\spark实验指导书\\test.txt")
    val rdd1 = dataRdd.map(_.split(",")).map(x => (x(3), 1))
      .filter(a => a._1.contains("女"))
      .reduceByKey(_ + _)

    rdd1.foreach(println)

  }


}
