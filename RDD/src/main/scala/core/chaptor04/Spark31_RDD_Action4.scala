package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark31_RDD_Action4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output")

    //TODO saveAsSequenceFile，必须是K-V型的RDD才可以调用
    rdd.map((_, 1)).saveAsSequenceFile("output")

    sc.stop()
  }
}
