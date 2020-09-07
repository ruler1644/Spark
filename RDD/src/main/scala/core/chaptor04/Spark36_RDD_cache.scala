package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

object Spark36_RDD_cache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.makeRDD(Array("spark"))

    //TODO cache 缓存
    //将RDD转换为携带当前时间戳不做缓存
    val rdd2: RDD[String] = rdd.map(s => s + System.currentTimeMillis())
    //    rdd2.collect().foreach(println)
    //    rdd2.collect().foreach(println)
    //    rdd2.collect().foreach(println)
    //spark1566441256338
    //spark1566441256456
    //spark1566441256537


    //将RDD转换为携带当前时间戳并做缓存
    val cacheRDD1: RDD[String] = rdd.map(s => s + System.currentTimeMillis()).cache()
    cacheRDD1.collect().foreach(println)
    cacheRDD1.collect().foreach(println)
    cacheRDD1.collect().foreach(println)
    //spark1596809088510
    //spark1596809088510
    //spark1596809088510



    //TODO checkPoint之前的依赖，血统
    //(4) MapPartitionsRDD[2] at map at Spark36_RDD_cache.scala:26 [Memory Deserialized 1x Replicated]
    // |       CachedPartitions: 4; MemorySize: 152.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
    // |  ParallelCollectionRDD[0] at makeRDD at Spark36_RDD_cache.scala:12 [Memory Deserialized 1x Replicated]
    val lineage1: String = cacheRDD1.toDebugString
    println(lineage1)
    println("--------")

    // List(org.apache.spark.OneToOneDependency@4351171a)
    val dependencies1: Seq[Dependency[_]] = cacheRDD1.dependencies
    println(dependencies1)
    println("--------")

    //TODO checkPoint
    //在checkpoint的过程中，该RDD的所有依赖于父RDD中的信息将全部被移除。
    //对RDD进行checkpoint操作并不会马上被执行，必须执行Action操作才能触发
    sc.setCheckpointDir("output")
    rdd2.checkpoint()

    rdd2.collect().foreach(println)
    rdd2.collect().foreach(println)
    rdd2.collect().foreach(println)
    rdd2.collect().foreach(println)
    println("**********")

    //spark1596809090231
    //spark1596809090732
    //spark1596809090732
    //spark1596809090732

    //(4) MapPartitionsRDD[1] at map at Spark36_RDD_cache.scala:16 []
    // |  ReliableCheckpointRDD[3] at collect at Spark36_RDD_cache.scala:53 []
    val lineage: String = rdd2.toDebugString
    println(lineage)
    println("**********")


    //List(org.apache.spark.OneToOneDependency@f4c0e4e)
    val dependencies: Seq[Dependency[_]] = rdd2.dependencies
    println(dependencies)
    println("**********")

  }
}