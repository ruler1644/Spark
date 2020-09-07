package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark28_RDD_Action1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 4, 2, 6, 4, 5), 2)

    // TODO Action算子 -collect
    //在驱动程序中，以数组的形式返回数据集的所有元素
    val arr: Array[Int] = rdd.collect
    for (i <- arr) {
      println(i)
    }
    println("*************")

    // TODO Action算子 -count
    //返回RDD中元素的个数
    val sum: Long = rdd.count
    println(sum)
    println("*************")

    // TODO Action算子 -first
    // 返回RDD中的第一个元素
    val i: Int = rdd.first()
    println(i)
    println("*************")

    // TODO Action算子 -take
    //返回一个由RDD的前n个元素组成的数组
    val array: Array[Int] = rdd.take(3)
    for (i <- array) {
      println(i)
    }
    println("*************")

    // TODO Action算子 -takeOrdered
    val arr2: Array[Int] = rdd.takeOrdered(4)
    for (i <- arr2) {
      println(i)
    }
    println("*************")
  }
}
