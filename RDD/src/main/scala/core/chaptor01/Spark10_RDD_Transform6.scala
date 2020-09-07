package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//sample(withReplacement, fraction, seed)
object Spark10_RDD_Transform6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 转换算子 - sample，数据倾斜的时候使用sample采样
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

    //val rdd1: RDD[Int] = rdd.sample(true, 0.4)
    val rdd2: RDD[Int] = rdd.sample(false, 0.5)
    rdd2.collect().foreach(println)

    sc.stop()
  }
}
