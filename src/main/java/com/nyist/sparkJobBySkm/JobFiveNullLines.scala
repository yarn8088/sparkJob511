package com.nyist.sparkJobBySkm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用累加器统计文件中有多少行空行
  */
object JobFiveNullLines {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BySkm")
      .setMaster("local[3]")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    val sc = new SparkContext(conf)



    //定义一个累加器
    val acc = sc.longAccumulator("MyAccumulator")
    val data = sc.textFile("C:\\Users\\21188\\Desktop\\spark实验指导书\\README.md")

   val result =  data.map(x=>(x,1)).map(a=>{
     if (a._1.isEmpty){
       acc.add(1)
     }
   })

    result.collect()
    print("文件中的空行数为    "+ acc.value)







  }
}
