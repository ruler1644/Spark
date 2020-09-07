package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Transform11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(4, 5, 6, 7, 8, 9), 3)

    //TODO 转换算子 - union,求并集1, 2, 3, 4 , 4, 5, 6, 7, 8, 9
    val resRDD1: RDD[Int] = rdd1.union(rdd2)
    resRDD1.collect().foreach(println)
    println("111111111111111")


    //TODO 转换算子 - subtract,求交集 4
    val resRDD2: RDD[Int] = rdd1.intersection(rdd2)
    resRDD2.collect().foreach(println)
    println("2222222222222222")

    //TODO 转换算子 - subtract,求差集
    //1,2,3
    val resRDD3: RDD[Int] = rdd1.subtract(rdd2)
    resRDD3.collect().foreach(println)
    println("333333333333333333")

    //5, 6, 7, 8, 9
    val resRDD4: RDD[Int] = rdd2.subtract(rdd1)
    resRDD4.collect().foreach(println)
    println("444444444444444444")


    // TODO 转换算子 - cartesian,笛卡尔积（尽量避免使用）
    //24行数据
    val resRDD5: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    resRDD5.collect().foreach(println)
    println("555555555555555555")

    //TODO 转换算子 - zip
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd4: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8), 2)

    //默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常
    //(1,5)
    //(2,6)
    //(3,7)
    //(4,8)
    val resRDD6: RDD[(Int, Int)] = rdd3.zip(rdd4)
    resRDD6.collect().foreach(println)
    println("66666666666666666")

  }
}
