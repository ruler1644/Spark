package core.chaptor01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark06_RDD_Transform2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)


    //TODO 扁平化flatMap,将整体拆分成一个一个的独立个体
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"), 2)

    //val flatMapRDD1: RDD[String] = rdd.flatMap(s =>s.split(" "))
    val flatMapRDD1: RDD[String] = rdd.flatMap(_.split(" "))

    //Hello
    //Hello
    //Scala
    //Spark
    flatMapRDD1.foreach(println)


    //1
    //2
    //3
    //4
    val rdd1: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)), 2)
    val resRDD: RDD[Int] = rdd1.flatMap(list => list)
    resRDD.collect().foreach(println)


    //1
    //2
    //3
    //4
    val rdd2 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val resRDD2: RDD[Int] = rdd2.flatMap(num => List(num))
    resRDD2.collect().foreach(println)

    sc.stop()
  }
}
