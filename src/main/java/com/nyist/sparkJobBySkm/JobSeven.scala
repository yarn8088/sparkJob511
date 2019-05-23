package com.nyist.sparkJobBySkm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: skm
  * @Date: 2019/5/22 10:06
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobSeven {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //      .setMaster("spark://master:7077")
      .setMaster("local[4]")
      .setAppName("BySkm")
    //     .setJars(Seq("target/sparkJob511-1.0-SNAPSHOT-jar-with-dependencies.jar"))

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val a = 2000000
    val data = sc.makeRDD(2 to a, 8)

    //    val result1 = data.map(x => (x, (2 to (a / x)))).flatMap(kv => kv._2.map(_ * kv._1))
    //重分区优化后
    val result1 = data.map(x => (x, (2 to (a / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1))

    val result2 = sc.makeRDD(2 to a, 8).subtract(result1)

//    result2.saveAsTextFile("hdfs://mycluster/dataBuilder/JobSenven1")
    //第二次的输出结果
    result2.saveAsTextFile("hdfs://mycluster/dataBuilder/JobSenven2")


  }


}
