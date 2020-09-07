package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark29_RDD_Action2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sc.makeRDD(list, 2)
    val sum: Int = rdd.aggregate(0)(_ + _, _ + _)

    //10
    println(sum)
    println("*************************")


    val sum2: Int = rdd.aggregate(10)(_ + _, _ + _)

    //40
    println(sum2)
  }

  def main2(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val list = List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //TODO Action算子 aggregate
    // aggregateByKey中的初始值在分区内计算有效
    // aggregate中的初始值在分区间也有效
    //参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)

    //(0,(a,3))
    //(0,(a,2))
    //(0,(c,4))
    //(1,(b,3))
    //(1,(c,6))
    //(1,(c,8))
    val indexRDD: RDD[(Int, (String, Int))] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data)
        )
      }
    )

    indexRDD.collect().foreach(println)
    println("*************************")

    //rdd.aggregate(10)(_+_._2,_+_)
    val num: Int = rdd.aggregate(10)(((x, y) => (x + y._2)), ((x, y) => (x + y)))

    //56
    println(num)
  }
}
