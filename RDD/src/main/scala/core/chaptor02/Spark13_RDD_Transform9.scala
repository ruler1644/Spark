package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Transform9 {

  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 4)
    val rdd1: RDD[Int] = rdd.repartition(3)

    val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data))
      }
    )

    //(0,6)
    //(0,7)
    //(1,1)
    //(1,3)
    //(1,8)
    //(2,2)
    //(2,4)
    //(2,5)
    rdd2.collect().foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)


    // TODO 转换算子 - repartition ,会shuffle
    // 从底层代码来将，其实就是shuffle为true的coalesce
    //coalesce(numPartitions, shuffle = true)
    val list = List((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))
    val rdd: RDD[(Int, String)] = sc.makeRDD(list, 3)
    val resRDD: RDD[(Int, (Int, String))] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data))
      }
    )
    //(0,(1,a))
    //(1,(2,b))
    //(1,(3,c))
    //(2,(4,d))
    //(2,(5,e))
    resRDD.collect().foreach(println)
    println("*************")


    val repartitionRDD: RDD[(Int, String)] = rdd.repartition(2)
    val resRDD2: RDD[(Int, (Int, String))] = repartitionRDD.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((data) => (index, data))
      }
    )
    //(0,(1,a))
    //(0,(2,b))
    //(0,(4,d))
    //(1,(3,c))
    //(1,(5,e))
    resRDD2.collect().foreach(println)
  }
}
