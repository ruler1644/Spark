package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Transform17_06 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val array = Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd = sc.makeRDD(array, 2)

    // TODO 转换算子 - combineByKey
    //参数：(createCombiner: V => C,  mergeValue: (C, V) => C,  mergeCombiners: (C, C) => C)
    // 第一个参数为初始值的计算规则
    // 第二个参数为分区内计算规则
    // 第三个参数为分区间计算规则
    //TODO  创建一个pairRDD，根据key计算每种key的均值。
    //（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
    // 0 - ("a", 88), ("b", 95), ("a", 91)
    //     (88,1) + 91 => (179,2)
    // 1 - ("b", 93), ("a", 95), ("b", 98)
    //     (95,1)
    // (a, (274/3))
    // (a, sum/count), (b, sum/count)

    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey(
      num => (num, 1),
      (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
      (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
    )
    rdd2.collect().foreach(println)
    println("*********************")

    val rdd3: RDD[(String, Int)] = rdd2.map {
      case (str, tup) => {
        (str, tup._1 / tup._2)
      }
    }

    //(b,95)
    //(a,91)
    rdd3.collect().foreach(println)
    println("*********************")

    val rdd4: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91),
      ("b", 93), ("a", 95), ("b", 98)), 2)
    //TODO 转换算子 - combineByKey，实现wordCount06

    val rdd5: RDD[(String, Int)] = rdd4.combineByKey(

      //num => num, _ + _, _ + _
      num => num, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y
    )

    //(b,286)
    //(a,274)
    rdd5.collect().foreach(println)
    sc.stop()
  }
}
