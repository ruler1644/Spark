package core.chaptor01

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Partition {

  // TODO 从集合创建RDD
  def main1(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)


    //数据数目 = 分区数目, (0,1)(1,2)(3,4)(4,5)
    //(1)(2)(3)(4)
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)

    //数据数目 > 分区数目,(1)(2,3)(4)(5,6)
    //(0,1)(1,3)(3,4)(4,8)?????
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 4)

    //数据数目 < 分区数目,(0,0)(0,1)(1,2)(2,3)
    //()(1)(2)(3)???????
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3), 4)

    /**
      * 数据分配策略源码
      * def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      * (0 until numSlices).iterator.map { i =>
      * val start = ((i * length) / numSlices).toInt
      * val end = (((i + 1) * length) / numSlices).toInt
      * (start, end)
      * }
      * }
      */

    //将数据保存到输出路径中
    rdd.saveAsTextFile("out")
    sc.stop()
  }

  // TODO 从磁盘文件中创建RDD
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    //从磁盘文件中创建RDD,分区数量是动态改变，取决于文件的大小
    //val rdd: RDD[String] = sc.textFile("input/1.txt")
    val rdd: RDD[String] = sc.textFile("input/1.txt", 2)

    //将数据保存到输出路径中
    rdd.saveAsTextFile("out/1")
    sc.stop()
  }
}
