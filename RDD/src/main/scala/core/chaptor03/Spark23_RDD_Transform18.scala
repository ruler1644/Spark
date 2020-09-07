package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Transform18 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))

    // TODO 转换算子 - sortByKey
    //(1,dd)
    //(2,bb)
    //(3,aa)
    //(6,cc)
    //正序
    //val rdd1: RDD[(Int, String)] = rdd.sortByKey(true)
    val rdd1: RDD[(Int, String)] = rdd.sortByKey(false)

    //(6,cc)
    //(3,aa)
    //(2,bb)
    //(1,dd)
    //倒序
    rdd1.collect().foreach(println)
  }
}
