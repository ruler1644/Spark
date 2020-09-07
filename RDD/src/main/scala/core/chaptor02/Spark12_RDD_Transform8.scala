package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Transform8 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 4)
    val rdd1: RDD[Int] = rdd.coalesce(3, true)

    val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data))
      }
    )

    //(0,1)
    //(0,2)
    //(1,3)
    //(1,4)
    //(2,5)
    //(2,6)
    //(2,7)
    //(2,8)

    rdd2.collect().foreach(println)

    //(0,6)
    //(0,7)
    //(1,1)
    //(1,3)
    //(1,8)
    //(2,2)
    //(2,4)
    //(2,5)
  }


  def main2(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 转换算子 - coalesce ,不会shuffle
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")), 3)

    val resRDD: RDD[(Int, (Int, String))] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data))
      }
    )

    //(0,(1,a))
    //(1,(2,b))
    //(2,(3,c))
    resRDD.collect().foreach(println)


    val coalesceRDD: RDD[(Int, String)] = rdd.coalesce(2)
    val resRDD2: RDD[(Int, (Int, String))] = coalesceRDD.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((data) => (index, data))
      }
    )

    //(0,(1,a))
    //(1,(2,b))
    //(1,(3,c))
    resRDD2.collect().foreach(println)
  }
}
