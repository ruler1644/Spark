package core.chaptor05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_BroadCast {

  //TODO 直接join产生shuffle效率非常低下
  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 11), ("b", 22), ("c", 33)))
    val res: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    //(a,(1,11))
    //(b,(2,22))
    //(c,(3,33))
    res.collect().foreach(println)
    sc.stop()

  }

  //TODO  遍历一个数据集，不会产生shuffle
  //但是产生闭包了，需要序列化list，并且会将其分发给每一个Task，效率也不高
  def main2(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 11), ("b", 22), ("c", 33)))

    val list = List(("a", 11), ("b", 22), ("c", 33))

    val res: RDD[(String, (Int, Int))] = rdd1.map {
      case (k1, v1) => {
        var v = 0
        for ((k2, v2) <- list) {
          if (k1 == k2) {
            v = v2
          }
        }
        (k1, (v1, v))
      }
    }

    //(a,(1,11))
    //(b,(2,22))
    //(c,(3,33))
    res.collect().foreach(println)
    sc.stop()

  }

  //TODO 广播变量
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 11), ("b", 22), ("c", 33)))

    //声明广播变量
    val list = List(("a", 11), ("b", 22), ("c", 33))
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val res: RDD[(String, (Int, Int))] = rdd1.map {
      case (k1, v1) => {
        var v = 0
        for ((k2, v2) <- broadcast.value) {
          if (k1 == k2) {
            v = v2
          }
        }
        (k1, (v1, v))
      }
    }

    //(a,(1,11))
    //(b,(2,22))
    //(c,(3,33))
    res.collect().foreach(println)
    sc.stop()

  }
}