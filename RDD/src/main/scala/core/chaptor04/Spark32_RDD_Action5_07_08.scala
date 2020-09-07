package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark32_RDD_Action5_07_08 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)


    //TODO  Action算子-countByKey,实现实现wordCount07
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 1), ("a", 1)), 3)

    val rdd1: RDD[String] = rdd.map {
      case (s, n) => (s + " ") * n
    }
    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))
    val wordRdd4: collection.Map[String, Long] = rdd3.countByKey()

    //(a,5)
    for (kv <- wordRdd4) {
      println("***********")
      println(kv)
    }


    //TODO  Action算子-countByValue,实现实现wordCount08
    val wordRDD: RDD[String] = sc.makeRDD(List("a", "a", "a"))
    val wordMap: collection.Map[String, Long] = wordRDD.countByValue()

    //***********
    //(a,3)
    for (kv <- wordMap) {
      println("***********")
      println(kv)
    }

  }
}
