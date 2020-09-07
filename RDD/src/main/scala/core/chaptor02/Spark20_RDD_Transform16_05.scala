package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Transform16_05{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    // TODO 转换算子 - foldByKey,实现wordCount05
    //当分区内计算规则和分区间计算规则相同时，可以使用foldByKey进行简化
    val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)((_ + _))

    //(b,3)
    //(a,5)
    //(c,18)
    rdd2.collect().foreach(println)
    sc.stop()
  }
}


