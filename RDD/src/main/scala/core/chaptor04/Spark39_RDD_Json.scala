package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Spark39_RDD_Json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //TODO 读取json数据
    val jsonRDD: RDD[String] = sc.textFile("input/2.json")
    val res: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull)
    res.collect.foreach(println)

  }
}