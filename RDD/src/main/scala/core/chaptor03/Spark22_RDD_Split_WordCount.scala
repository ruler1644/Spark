package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// TODO 数据切分，实现WordCount
object Spark22_RDD_Split_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    println("hello " * 3)
    println("************************")


    val list = List(("Hello Hadoop Spark Scala", 4), ("Hello Hadoop Spark", 3), ("Hello Hadoop", 2), ("Hello", 1))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list)


    //第一，二种切分方式,map将字符变成长字符
    //("Hello Hadoop Spark Scala", 4) =>
    //Hello Hadoop Spark Scala Hello Hadoop Spark Scala Hello Hadoop Spark Scala Hello Hadoop Spark Scala
    val rddMatch: RDD[String] = rdd.map {
      case (s, n) => (s + " ") * n
    }

    rddMatch.collect().foreach(println)
    println("************************")

    val rdd1: RDD[String] = rdd.map(tup => (tup._1 + " ") * tup._2)
    rdd1.collect().foreach(println)
    println("************************")

    //第三，四种切分方式，先flatMap扁平化，再map将长字符切成单个字符，加上数量
    //("Hello Hadoop Spark Scala", 4) =>
    //(Hello,4)
    //(Hadoop,4)
    //(Spark,4)
    //(Scala,4)
    val rddFlatMap: RDD[(String, Int)] = rdd.flatMap {
      case (line, num) => {
        line.split(" ").map(
          word => (word, num)
        )
      }
    }
    rddFlatMap.collect().foreach(println)
    println("************************")

    val rddFlatMap22: RDD[(String, Int)] = rdd.flatMap {
      (tup) => {
        tup._1.split(" ").map {
          word => (word, tup._2)
        }
      }
    }
    rddFlatMap22.collect().foreach(println)
    println("************************")
    rddFlatMap22.reduceByKey(_ + _).collect().foreach(println)
  }
}
