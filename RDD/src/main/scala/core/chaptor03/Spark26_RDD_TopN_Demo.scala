package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//每个省份广告被点击数次top3
object Spark26_RDD_TopN_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ahjd")
    val sc = new SparkContext(conf)

    //TODO 读取日志数据
    //TODO 改变数据结构map        line----------(pri-adv,1)
    //TODO 分组聚合reduceByKey    (pri-adv,1)----(pri-adv,sum)
    //TODO 改变数据结构map        (pri-adv,sum)---(pri,adv-sum)
    //TODO 分组groupByKey         (pri,adv-sum)---(pri,Iterable[adv-sum])
    //TODO 对Value的第二个字段排序mapValues


    val rdd: RDD[String] = sc.textFile("input/agent.log")

    //1516609143867 6 7 64 16
    //1516609143869 9 4 75 18

    // TODO 将数据进行转换，保留功能所有的数据
    val mapRDD: RDD[(String, Int)] = rdd.map(line => {
      val datas: Array[String] = line.split(" ")

      // TODO 将数据进行结构的转换 （pri-adv, 1）
      (datas(1) + "-" + datas(4), 1)
    })

    // TODO 将转换结构后的数据进行分组聚合 （pri-adv, 1）=> （pri-adv, sum）
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey((x, y) => (x + y))

    // TODO 将聚合的结果进行结构的转换（pri-adv, sum） => ( pri, (adv, sum) )
    val reduceMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) => {
        val data: Array[String] = key.split("-")
        (data(0), (data(1), sum))
      }
    }

    // TODO 将转换结构后的数据进行分组 ( pri, (adv, sum) ) => ( pri, Iterator[ (adv, sum) ] )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = reduceMapRDD.groupByKey()

    // TODO 将分组后的数据进行排序（降序），取前三名
    //排序时，不关心key，只关注value。
    //datas是可迭代的集合，但是不一定可以排序。先变成有序的集合，之后才可以排序
    //降序是左边left元素大于右边right元素，而left和right都是tuple类型，tuple不能直接比较
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })
    resRDD.collect().foreach(println)
    sc.stop()
  }
}
