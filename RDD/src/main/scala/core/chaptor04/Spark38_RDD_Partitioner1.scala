package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark38_RDD_Partitioner1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, Int)] = sc.makeRDD(Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)), 3)


    //(0,(1,1))
    //(0,(2,2))
    //(1,(3,3))
    //(1,(4,4))
    //(2,(5,5))
    //(2,(6,6))
    val indexRDD: RDD[(Int, (Int, Int))] = rdd.mapPartitionsWithIndex {
      case (index, datas) => {
        datas.map { (data) =>
          (index, data)
        }
      }
    }
    indexRDD.collect().foreach(println)


    //(0,(2,2))
    //(0,(4,4))
    //(0,(6,6))
    //(1,(1,1))
    //(1,(3,3))
    //(1,(5,5))
    val partitionRDD: RDD[(Int, Int)] = rdd.partitionBy(new myPartitioner(2))
    val indexRDD2: RDD[(Int, (Int, Int))] = partitionRDD.mapPartitionsWithIndex {
      case (index, datas) => {
        datas.map { (data) =>
          (index, data)
        }
      }
    }
    indexRDD2.collect().foreach(println)
  }
}

class myPartitioner(numParts: Int) extends Partitioner {

  //返回创建出来的分区数
  override def numPartitions: Int = {
    numParts
  }

  //返回给定键的分区编号(0到numPartitions-1)
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numParts
  }
}