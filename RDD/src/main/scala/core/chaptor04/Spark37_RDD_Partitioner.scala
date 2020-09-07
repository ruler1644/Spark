package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark37_RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val list = List(("Hello", 4), ("Hello", 3), ("world Hadoop", 2), ("hello", 1))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //TODO 分区器，分区号
    val indexRDD: RDD[(Int, (String, Int))] = rdd.mapPartitionsWithIndex {
      (index, datas) => {
        datas.map(data => (index, data))
      }
    }
    indexRDD.collect().foreach(println)
    //(0,(Hello,4))
    //(0,(Hello,3))
    //(1,(Hello Hadoop,2))
    //(1,(Hello,1))
    //None
    val partitioner: Option[Partitioner] = rdd.partitioner
    println(partitioner)


    //TODO 分区器，分区号
    val partitionRDD: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(3))
    val indexRDD2: RDD[(Int, (String, Int))] = partitionRDD.mapPartitionsWithIndex {
      (index, datas) => {
        datas.map(data => (index, data))
      }
    }
    indexRDD2.collect().foreach(println)

    //(1,(world Hadoop,2))
    //(1,(hello,1))
    //(2,(Hello,4))
    //(2,(Hello,3))
    //Some(org.apache.spark.HashPartitioner@3)
    val partitioner2: Option[Partitioner] = partitionRDD.partitioner
    println(partitioner2)

  }
}
