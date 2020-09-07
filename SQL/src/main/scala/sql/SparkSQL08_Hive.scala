package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL08_Hive {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("agjd")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //spark内部的hive
    spark.sql("show tables").show()

    val rdd: RDD[(Int, Int)] = spark.sparkContext.makeRDD(List((1, 20), (2, 30)))
    import spark.implicits._
    val df: DataFrame = rdd.toDF("id", "age")
    df.createOrReplaceTempView("user")

    //spark内部的hive，再次查询
    spark.sql("show tables").show()

  }

}
