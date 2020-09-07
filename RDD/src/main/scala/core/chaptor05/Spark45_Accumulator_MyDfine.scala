package core.chaptor05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object Spark45_Accumulator_MyDfine {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    //创建累加器
    val accumulator = new MyAccumulator

    //注册累加器
    sc.register(accumulator, "我的累加器")
    val wordRDD: RDD[String] = sc.makeRDD(List("Scala", "Spark", "Hive", "Scala"))

    wordRDD.foreach(str => {
      if (str.startsWith("S")) {
        accumulator.add(str)
      }
    })

    //获取累加器的值
    println(accumulator.value)
    sc.stop()
  }
}

//敏感词过滤
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {
  var wordMap = mutable.Map[String, Int]()

  //是否为初始状态
  override def isZero: Boolean = {
    wordMap.isEmpty
  }

  //复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new MyAccumulator
  }

  //重置累加器
  override def reset(): Unit = {
    wordMap.clear()
  }

  //累加数据
  override def add(v: String): Unit = {
    wordMap(v) = wordMap.getOrElse(v, 0) + 1
  }

  //在Driver端合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

    //合并两个map
    val map1 = wordMap
    val map2 = other.value

    wordMap = map1.foldLeft(map2)(
      (map, kv) => {
        map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
        map
      }
    )
  }

  //累加器的值
  override def value: mutable.Map[String, Int] = {
    wordMap
  }
}
