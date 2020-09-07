package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL07_JDBC {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("agjd")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //Spark读取MySQL数据库中的数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      .option("dbtable", "tmp")
      .option("user", "root")
      .option("password", "root")
      .load()

    df.show()


    //将数据写入mysql
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((12, "tom", 36)))

    import spark.implicits._

    val saveDF: DataFrame = rdd.toDF("id", "name", "age")

    saveDF.write.format("jdbc").mode("append")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      .option("dbtable", "tmp")
      .option("user", "root")
      .option("password", "root")
      .save()

  }
}
