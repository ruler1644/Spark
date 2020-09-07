package core.chaptor03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Transform20 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List((1, "a"), (2, "b"),(1, "a")))
    val rdd2 = sc.makeRDD(List((2, "d"), (1, "c"), (3, "e")))

    // TODO 转换算子 - join, 相当于从笛卡儿积中过滤数据
    // 默认的连接数据的条件是key相同
    //(1,(a,c))
    //(1,(a,c))
    //(2,(b,d))
    val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    rdd3.collect().foreach(println)


    // TODO 转换算子 - cogroup
    //(1,(CompactBuffer(a, a),CompactBuffer(c)))
    //(2,(CompactBuffer(b),CompactBuffer(d)))
    //(3,(CompactBuffer(),CompactBuffer(e)))
    val rdd4: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2)
    rdd4.collect().foreach(println)
  }
}
