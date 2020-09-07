package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_RDD_Action {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 10, 2)
    val num: Int = rdd.reduce((x, y) => x + y)

    //55
    println(num)


    val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

    //分区信息
    //(0,(a,1))
    //(0,(a,3))
    //(1,(c,3))
    //(1,(d,5))
    val rdd3: RDD[(Int, (String, Int))] = rdd2.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(data => (index, data)
        )
      }
    )
    rdd3.collect().foreach(println)
    println("********************")

    // TODO Action算子 - reduce聚合
    //通过func函数，聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    //rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val tuple1: (String, Int) = rdd2.reduce {
      case ((x, y), (m, n)) => (x + m, y + n)
    }

    //(aa,4)----->
    //(cd,8)----->(dc,8)
    //(cdaa,12)
    println(tuple1)
  }
}
