package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Transform19 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "b"), (1, "c"), (2, "c")))

    //value值乘以2----原始方式
    val rdd1: RDD[(Int, String)] = rdd.map {
      case (n, str) => (n, str * 2)
    }
    //(1,aa)
    //(1,bb)
    //(1,cc)
    rdd1.collect().foreach(println)
    println("********************")

    // TODO 转换算子 - mapValues
    val rdd2: RDD[(Int, String)] = rdd.mapValues((str) => str * 2 + "HH")

    //(1,aaHH)
    //(1,bbHH)
    //(1,ccHH)
    rdd2.collect().foreach(println)
    println("********************")


    //value是否含有a----原始方式
    val rdd3: RDD[(Int, Iterable[String])] = rdd.groupByKey()
    val rdd4: RDD[(Int, Iterable[String])] = rdd3.map {
      case (key, list) => {
        (key, list.filter(str => str.contains("a")))
      }
    }
    //(1,List(a))
    //(2,List())
    rdd4.collect().foreach(println)
    println("********************")


    // TODO 转换算子 - mapValues
    val rdd5: RDD[(Int, Iterable[String])] = rdd3.mapValues(
      lists => lists.filter(_.contains("a"))
    )
    //(1,List(a))
    //(2,List())
    rdd5.collect().foreach(println)
    println("********************")

  }
}
