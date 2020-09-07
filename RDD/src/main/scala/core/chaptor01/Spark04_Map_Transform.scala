package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Map_Transform {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(config)

    //map映射，将数据进行结构的转换，V --> K,V
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 1))
    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1)) //(1,1),(2,1),(3,1),(1,1)

    //都是RDD，在map之后才可以操作key，是因为存在隐式转换
    val value: RDD[(Int, Int)] = mapRdd.reduceByKey(_ + _) //(1,2),(2,1),(3,1)

    println(value.collect().mkString(","))


    sc.stop()
  }
}
