package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//TODO 自定义UDF函数，在姓名前添加前缀Name
object SparkSQL03_UDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.read.json("input/people.json")

    //定义并且注册函数
    spark.udf.register("prefix", (str: String) => ("Name: " + str))

    //使用函数
    df.createOrReplaceTempView("user")
    spark.sql("select prefix(name) from user").show()
    spark.close()
  }
}
