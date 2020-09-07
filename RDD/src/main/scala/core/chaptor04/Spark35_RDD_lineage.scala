package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

object Spark35_RDD_lineage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    val rdd1: RDD[(String, Int)] = rdd.map((_, 1))
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)

    //(4) ShuffledRDD[2] at reduceByKey at Spark35_RDD_lineage.scala:11 []
    // +-(4) MapPartitionsRDD[1] at map at Spark35_RDD_lineage.scala:11 []
    //    |  ParallelCollectionRDD[0] at parallelize at Spark35_RDD_lineage.scala:10 []
    val lineage1: String = rdd2.toDebugString
    println(lineage1)
    println("**************")

    //List(org.apache.spark.ShuffleDependency@c81fd12)
    //List(org.apache.spark.OneToOneDependency@62e6a3ec)
    val dependencies1: Seq[Dependency[_]] = rdd2.dependencies
    val dependencies2: Seq[Dependency[_]] = rdd1.dependencies
    println(dependencies1)
    println(dependencies2)
    println("**************")

    rdd2.collect
  }
}