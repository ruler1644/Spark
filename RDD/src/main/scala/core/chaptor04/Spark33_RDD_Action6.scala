package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark33_RDD_Action6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //1
    //2
    //3
    //4
    rdd.collect().foreach(println)
    println("*******************")



    //TODO  Action算子-foreach
    //RDD的算子逻辑执行是分布式的，所以打印顺序是无序的
    //1
    //4
    //2
    //3
    rdd.foreach(println)
    sc.stop()




  }
}
