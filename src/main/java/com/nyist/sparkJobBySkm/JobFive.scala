package com.nyist.sparkJobBySkm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: skm
  * @Date: 2019/5/16 20:08
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobFive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("BySkm")
    //将日志等级关闭，看上去累赘
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)


    val sc = new SparkContext(conf)

    //将文件读取为rdd进行操作
    val data = sc.textFile("C:\\Users\\21188\\Desktop\\spark实验指导书\\score.txt")

    //定义一个累加器
    val accumulator = sc.longAccumulator("myAccumulator")

    val accmuData = data.map(_.split(",")).map(x => (x(1), 1)).filter(_._1.contains("DataBase"))
//      .reduceByKey(_+_)

    val resultRdd = accmuData.map(
      x => {
        if (x._2.toInt == 1) {
          accumulator.add(1)
        }
//        (x._1, accumulator.value)

      })
//          accmuData.foreach(println)
        resultRdd.collect()
        print(("DataBase",accumulator.value))


  }

}
