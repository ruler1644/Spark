package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL02_Transform {
  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("gsjd")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark.read.json("input/people.json")

    // TODO 1. RDD转DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30)))

    import spark.implicits._

    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.show()

    //TODO 2. DataFrame转RDD
    val rdd2: RDD[Row] = df.rdd
    rdd2.collect().foreach(row => {
      println(row.get(0))
      println(row.get(1))
      println(row.get(2))
    })

    //TODO 3. RDD转DataSet
    //将数据类型转换为样例类对象
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    userDS.show()

    //TODO 4. DataSet转RDD
    //RDD存储的是User对象
    val resRDD: RDD[User] = userDS.rdd
    resRDD.collect().foreach(user => {
      println(user)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("gsjd")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //TODO 5. DataFrame转DataSet
    //增加类型信息
    val df: DataFrame = spark.read.json("input/people.json")
    val ds: Dataset[Person1] = df.as[Person1]
    ds.show()


    //TODO 6. DataSet转DataFrame
    val df2: DataFrame = ds.toDF()
    df2.show()

    // 释放资源
    spark.close()
  }
}

case class User(id: Int, name: String, age: BigInt)

case class Person1(name: String, age: BigInt)

