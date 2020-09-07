package core.chaptor02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark16_RDD_Transform12 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)


    val rdd: RDD[(Int, String)] = sc.makeRDD(List((11, "a"), (12, "b"), (13, "c")), 3)
    val rdd2: RDD[(Int, (Int, String))] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((str) => (index, str))
      }
    )

    //(0,(11,a))
    //(1,(12,b))
    //(2,(13,c))
    rdd2.collect().foreach(println)
    println("*********************")


    //TODO 转换算子 - partitionBy,需要传分区器
    val rdd1: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))

    val rdd3: RDD[(Int, (Int, String))] = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((str) => (index, str))
      }
    )

    //(0,(2,b))
    //(1,(1,a))
    //(1,(3,c))
    rdd3.collect().foreach(println)
    println("*********************")

    val rdd4: RDD[(Int, String)] = rdd.partitionBy(new myPartition(2))

    val rdd5: RDD[(Int, (Int, String))] = rdd4.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((num) => (index, num))
      })

    //(1,(11,a))
    //(1,(12,b))
    //(1,(13,c))
    rdd5.collect().foreach(println)
  }
}

//TODO 转换算子 -自定义分区器
class myPartition(num: Int) extends Partitioner {

  //主构造函数体，类体


  //获取分区数量
  override def numPartitions: Int = {
    num
  }

  //根据key返回分区索引号
  override def getPartition(key: Any): Int = {
    //(0,(12,b))
    //(1,(11,a))
    //(1,(13,c))
    //key.toString.toInt % num

    //(1,(11,a))
    //(1,(12,b))
    //(1,(13,c))
    1
  }
}
