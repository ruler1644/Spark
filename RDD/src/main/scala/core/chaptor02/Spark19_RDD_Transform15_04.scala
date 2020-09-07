package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Transform15_04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val list = List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    // TODO 取出每个分区相同key对应值的最大值，然后相加
    //参数：(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U)

    //val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => (x + y))
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), (_ + _))

    //(b,3)
    //(a,3)
    //(c,12)
    rdd1.collect().foreach(println)
    println("*************")

    // TODO 转换算子 - aggregateByKey,实现wordCount04
    //val rdd3: RDD[(String, Int)] = rdd.aggregateByKey(0)((x, y) => (x + y), (x, y) => (x + y))
    val rdd3: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)

    //(b,3)
    //(a,5)
    //(c,18)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}


