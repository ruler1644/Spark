package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL06_Data {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sgasl")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取json格式的文件
    //val df: DataFrame = spark.read.load("input/people.json")
    val df: DataFrame = spark.read
      .format("json")
      .load("input/people.json")

    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()


    //通用的保存数据的方式
    //df.write.mode("overwrite").format("json").save("input/save")
    df.write
      .mode("append")
      .format("json")
      .save("input/save")

    spark.close()
  }

}
