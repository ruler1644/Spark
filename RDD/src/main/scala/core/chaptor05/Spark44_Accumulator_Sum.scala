package core.chaptor05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark44_Accumulator_Sum {

  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //在Driver端执行
    var sum = 0
    rdd.foreach(num => {

      // 更新sum的值，在Executor端执行
      sum = sum + num
    })

    //Driver可以传数据给Executor，但是Executor不能传数据Driver
    //Driver端sum的值不会改变
    println(sum)
    sc.stop()

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    //创建累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.foreach(num => {

      //更新累加器的值
      sum.add(num)
    })

    //获取累加器的值
    println(sum.value)

    sc.stop()
  }
}
