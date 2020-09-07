package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Transform13_02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c"), (1, "c")))

    //(1,CompactBuffer(a, c))
    //(2,CompactBuffer(b))
    //(3,CompactBuffer(c))
    val rdd1: RDD[(Int, Iterable[String])] = rdd.groupByKey()
    rdd1.collect().foreach(println)

    //TODO 转换算子 - groupByKey,实现wordCount02
    //groupByKey对每个key进行操作，但只生成一个sequence

    val wordRDD = sc.makeRDD(List("Hello", "Hello", "Spark", "Spark"))
    val wordRDD2: RDD[(String, Int)] = wordRDD.map(s => (s, 1))
    val wordRDD3: RDD[(String, Iterable[Int])] = wordRDD2.groupByKey()

    val wordRDD4: RDD[(String, Int)] = wordRDD3.map {

      tuple => (tuple._1, tuple._2.sum)
      /*case (word, list) => {
        (word, list.size)
      }*/
    }

    //(Spark,2)
    //(Hello,2)
    wordRDD4.collect().foreach(println)
  }
}


