package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark30_RDD_Action3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //TODO Action算子 aggregate与 fold比较
    //40
    val rddNum: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val res1: Int = rddNum.aggregate(10)((_ + _), (_ + _))
    println(res1)
    //TODO Action算子 fold
    //40
    val res2: Int = rddNum.fold(10)(_ + _)
    println(res2)
  }
}
