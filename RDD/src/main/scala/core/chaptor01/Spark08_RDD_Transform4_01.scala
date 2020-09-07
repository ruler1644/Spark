package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Transform4_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    // TODO 转换算子 - groupBy
    //(分组key,集合类型)
    //(4,CompactBuffer(4))
    //(2,CompactBuffer(2, 2))
    //(1,CompactBuffer(1, 1))
    //(3,CompactBuffer(3))
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2), 2)
    val rdd1: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => num)
    rdd1.collect().foreach(println)

    // TODO 转换算子 - groupBy,实现wordCount01

    //(Spark,1)
    //(Hello,2)
    //(Scala,1)
    val wordRDD: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Spark", "Scala"))
    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(s => s)

    // case (name, age, email)，整体表示一个tuple类型的参数
    // 而(A,B) => C，表示有两个传入参数
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (str, list) => {
        (str, list.size)
      }
    }

    resRDD.collect() foreach (println)
    sc.stop()
  }
}
