import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: skm
  * @Date: 2019/5/22 8:29
  * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
  */
class SecondarySort(val one: Int, val two: Int) extends Ordered[SecondarySort] with Serializable {
  //比较
  override def compare(that: SecondarySort): Int = {
    //（oneData ， twoData）
    //第一数据不相等，直接返回大小
    if (this.one - that.one != 0) {
      this.one - that.one
    }

    //第一个数据相等，比较第二个数据
    else {
      this.two - that.two
    }


  }
}

object TwoSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BySkm")
      .setMaster("local[3]")

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://mycluster/dataBuilder/file1.txt")

    //将数据转换后的形式是(SecondarySort(8,3),"8 3")，其中键为定义的类，值为原来的数据
    val result = data.map(line => (new SecondarySort(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))

    //对key进行排序，然后只得到value即可
    val re = result.sortByKey().map(a => a._2)

    re.foreach(println)
  }
}
