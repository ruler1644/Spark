package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Transform14_03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Spark", "Spark"))


    // TODO 转换算子 - reduceByKey,实现wordCount03
    //val value: RDD[(String, Int)] = rdd.map((_,1))
    val rdd1: RDD[(String, Int)] = rdd.map((s) => (s, 1))

    //val value: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    //(Spark,2)
    //(Hello,2)
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey((n1, n2) => (n1 + n2))
    rdd2.collect().foreach(println)
    sc.stop()
  }
}


