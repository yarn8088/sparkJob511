package com.nyist.sparkJobBySkm

import org.apache.spark.sql.SparkSession

/**
  * @Author: skm
  * @Date: 2019/5/13 20:33
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobFour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BySkm")
      .master("local[4]")
      .getOrCreate()

    val dataFrame1 = spark.read.json("C:\\Users\\21188\\Desktop\\spark实验指导书\\employee.json")
    //查询所有数据
    dataFrame1.show()

    //将dataFrame注册为一张表
    dataFrame1.createOrReplaceTempView("employee")
    //查询所有数据，去掉重复数据
    spark.sql("select distinct(id),name,age from employee").show()
    //方法一：略微lowB  利用sql语句得到age>30的数据
    spark.sql("select * from employee where age > 30").show()

    //高大上的方法二：将隐式转换将RDD的操作用在dataFrame上
    import spark.implicits._
    dataFrame1.filter($"age" > 30).show()

    //将数据按照name降序排序
    spark.sql("select * from employee order by name desc").show()

    //取出前三行数据
    dataFrame1.filter($"id"<4).show()

    //查询出age的平均值
    spark.sql("select avg(age) from employee").show()
  }

}
