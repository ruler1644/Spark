package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Transform1 {

  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //分区数据范围(0,2)(2,4)，每个分区两条数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //TODO 转换算子 - map
    //每一个数据都会执行一次map，计数器1共输出4次
    val rdd1: RDD[Int] = rdd.map((num) => {
      println("计数器1")
      num * 2
    })

    val rdd2: Array[Int] = rdd1.collect()
    println(rdd2.mkString(","))
    sc.stop()
  }

  def main2(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //TODO 转换算子 - mapPartitions
    //每一个分区的数据执行一次map，计数器2共输出2次
    val rdd1: RDD[Int] = rdd.mapPartitions(datas => {
      println("计数器2")
      datas.map(num => {
        2 * num
      })
    })

    val rdd2: Array[Int] = rdd1.collect()
    println(rdd2.mkString(","))
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //TODO 转换算子 - mapPartitionsWithIndex
    //分区编号从0开始
    //(0,1)
    //(0,2)
    //(1,3)
    //(1,4)
    val rdd1: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((num) => (index, num))
      }
    )

    rdd1.collect().foreach(println)
    sc.stop()
  }
}