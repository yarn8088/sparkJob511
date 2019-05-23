package com.nyist.sparkJobBySkm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


/**
  * @Author: skm
  * @Date: 2019/5/23 11:29
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
object JobEight {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("BySkmq")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "hdfs://mycluster/user/hive/warehouse")
      .getOrCreate()
    //读取数据为RDD
    val tbDate = spark.sparkContext.textFile("hdfs://mycluster/dataBuilder/tbDate.txt")
    val tbStock = spark.sparkContext.textFile("hdfs://mycluster/dataBuilder/tbStock.txt")
    val tbStockDetail = spark.sparkContext.textFile("hdfs://mycluster/dataBuilder/tbStockDetail.txt")
    //将RDD转换为dataFrame并且注册为表

    //===============tbD表=================
    val fields = Array(StructField("date", StringType, true), StructField("years", StringType, true))
    //设置约束
    val dateSchema = StructType(fields)
    //将约束加在rdd上形成row类型的rdd
    val dateRow = tbDate.map(_.split(",")).map(x => Row(x(0), x(2)))
    //创建为dataframe
    val dataF = spark.createDataFrame(dateRow, dateSchema)
    //创建为表tbD
    dataF.createOrReplaceTempView("tbD")

    //=====================tbS表================
    val fields2 = Array(StructField("orderID", StringType, true), StructField("date", StringType, true))
    //设置约束
    val dateSchema2 = StructType(fields2)
    //将约束加在rdd上形成row类型的rdd
    val dateRow2 = tbStock.map(_.split(",")).map(x => Row(x(0), x(2)))
    //创建为dataframe
    val dataF2 = spark.createDataFrame(dateRow2, dateSchema2)
    //创建为表tbS
    dataF2.createOrReplaceTempView("tbS")


    //========================tbSD表================
    val fields3 = Array(StructField("orderID", StringType, true), StructField("orderNum", DoubleType, true), StructField("money", DoubleType, true))
    //设置约束
    val dateSchema3 = StructType(fields3)
    //将约束加在rdd上形成row类型的rdd
    val dateRow3 = tbStockDetail.map(_.split(",")).map(x => Row(x(0), x(3).toDouble, x(5).toDouble))
    //创建为dataframe
    val dataF3 = spark.createDataFrame(dateRow3, dateSchema3)
    //创建为表tbSD
    dataF3.createOrReplaceTempView("tbSD")


    //实验需求一：计算所有订单中每年的销售单数，销售总额
    val sqlString1 = "select c.years,count(distinct a.orderID),sum(b.money) from tbS a " +
      "join tbSD b on a.orderID=b.orderID " +
      "join tbD c on a.date=c.date group by c.years order by c.years"

//        spark.sql(sqlString1).show()

    //实验需求二：计算所有订单每年最大金额订单的销售额
    //    先按照日期和订单号进行分组计算，获取所有订单每天的数据
    //将其作为子查询
    //按照年份进行分组计算，使用max函数获取所有订单每年最大金额订单的销售额
    //
    //    val sqlString2 = "select a.date,a.orderID,sum(b.money) as summoney from tbS a, tbSD b " +
    //      "where a.orderID=b.orderID group by a.date,a.orderID"

    val sqlString2 = "select c.years,max(d.summoney) from tbD c," +
      "(select a.date,a.orderID,sum(b.money) as summoney from tbS a, tbSD b where a.orderID=b.orderID group by a.date, a.orderID) d " +
      "where c.date=d.date group by c.years sort by c.years"

//    spark.sql(sqlString2).show()

    //实验需求三：计算所有订单中每年最畅销的货品
    //第一步:select c.years,b.orderID,sum(b.money) as summoney from tbS a,tbSD b,tbD c where a.ordeID=b.orderID and
    //a.date=c.date group by c.years,b.orderID;
    //第二步：select d.years,max(d.summoney) as maxmoney from (select c.years,b.orderID,sum(b.money) as summoney from tbS a,tbSD b,tbD c where a.orderID=b.orderID and a.date=c.date group by c.years,b.orderID) d group by d.years ;

    val sqlString3 = "select distinct  e.years,e.orderID,f.maxmoney from " +
      "(select c.years,b.orderID,sum(b.money) as summoney from tbS a,tbSD b,tbD c where a.orderID=b.orderID and a.date=c.date group by c.years,b.orderID) e ," +
      "(select d.years,max(d.summoney) as maxmoney from (select c.years,b.orderID,sum(b.money) as summoney from tbS a,tbSD b,tbD c where a.orderID=b.orderID and a.date=c.date group by c.years,b.orderID) d group by d.years) f " +
      "where e.years=f.years and e.summoney=f.maxmoney order by e.years"

    spark.sql(sqlString3).show()



  }

}
