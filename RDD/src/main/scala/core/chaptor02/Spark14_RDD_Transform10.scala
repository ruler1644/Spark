package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Transform10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 转换算子 - sortBy
    val rdd: RDD[String] = sc.makeRDD(List("1", "11", "2", "22"))

    //ascending: Boolean = true,默认升序
    //1
    //2
    //11
    //22
    //val resRDD2: RDD[String] = rdd.sortBy(s => s.toInt)
    val resRDD2: RDD[String] = rdd.sortBy(s => s.toInt, false)
    resRDD2.collect().foreach(println)
  }
}
