package core.chaptor01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark07_RDD_Transform3 {

  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //TODO glom,每一个分区形成一个数组
    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4, 6, 5), 2)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    rdd1.collect().foreach(println)
  }

  def main2(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //TODO glom,每一个分区形成一个数组
    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4, 6, 5), 2)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    val rdd2: RDD[(Int, String)] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data.mkString("*")))
      }
    )

    //(0,1*3*2)
    //(1,4*6*5)
    rdd2.collect().foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //TODO glom,每一个分区形成一个数组
    //val sum1 = rdd.glom().collect().map(_.max).sum

    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4, 6, 5), 2)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    val array: Array[Array[Int]] = rdd1.collect()
    val sum = array.map(x => x.max).sum
    println(sum)

  }
}
