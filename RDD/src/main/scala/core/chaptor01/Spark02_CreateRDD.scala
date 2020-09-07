package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_CreateRDD {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(config)

    // TODO 从内存中创建RDD
    // 源码的角度来将，makeRDD底层调用的就是parallelize
    //val collectRDD0: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
     val collectRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
     val rdd: String = collectRDD.collect.mkString(",")
     println(rdd)
     sc.stop()

    // TODO 从磁盘文件中创建RDD
    //val FileRDD0: RDD[String] = sc.textFile("hdfs://192.168.183.101:9000/input/1.txt")
    val FileRDD1: RDD[String] = sc.textFile("input/words.txt")
    println(FileRDD1.collect().mkString(","))

    sc.stop()
  }
}
