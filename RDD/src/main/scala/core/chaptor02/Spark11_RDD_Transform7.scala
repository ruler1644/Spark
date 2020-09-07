package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Transform7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 1, 2, 3), 2)
    // TODO 转换算子 - distinct
    val rdd1: RDD[Int] = rdd.distinct()
    //2
    //1
    //3
    rdd1.collect().foreach(println)


    //默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它
    val rdd2: RDD[Int] = rdd.distinct(2)
    val resRDD: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((num) => (index, num))
      }
    )

    //(0,2)
    //(1,1)
    //(1,3)
    resRDD.collect().foreach(println)
    sc.stop()
  }
}
