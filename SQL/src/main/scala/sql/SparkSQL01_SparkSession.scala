package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_SparkSession {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("haha")

    //获取上下文环境对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取json文件，创建DataFrame
    val df: DataFrame = spark.read.json("input/people.json")

    //使用SQL语法访问DataFrame
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show

    //使用DSL语法访问DataFrame
    df.select("name").show()

    spark.close()
  }
}
