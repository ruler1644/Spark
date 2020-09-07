package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Transform5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    // TODO 转换算子 - filter
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //val rdd1: RDD[Int] = rdd.filter(num => num % 2 == 0)
    val rdd1: RDD[Int] = rdd.filter(_ % 2 == 0)

    //2
    //4
    rdd1.collect().foreach(println)
  }
}
