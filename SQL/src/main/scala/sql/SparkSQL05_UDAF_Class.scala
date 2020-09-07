package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator}
import org.apache.spark.sql.{Encoder, _}

//TODO 强类型UDAF聚合函数
object SparkSQL05_UDAF_Class {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("hahrtha")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("input/people.json")

    // 创建聚合函数
    val udaf = new AgeAvgClassUDAF

    //强类型聚合函数无法在SQL中执行(若可以执行，传入的是age,但输入类型是Person不一致)
    //spark.sql("select ageAvg(age) from user").show

    // 将聚合函数当作查询的列(传入Person，返回Double)
    val col: TypedColumn[Person, Double] = udaf.toColumn.name("ageAvg")

    //使用DSL语法来访问聚合函数，但是DataFrame没有类型信息，需要转成DataSet
    val ds: Dataset[Person] = df.as[Person]

    ds.select(col).show()
    spark.close()
  }
}

case class Person(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Long)

//TODO 继承 Aggregator, 声明泛型，重写六个方法
class AgeAvgClassUDAF extends Aggregator[Person, AvgBuffer, Double] {

  // 缓冲区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L, 0L)
  }

  // 聚合数据
  override def reduce(buffer: AvgBuffer, person: Person): AvgBuffer = {
    buffer.sum = buffer.sum + person.age
    buffer.count = buffer.count + 1L
    buffer
  }

  // 合并缓冲区
  override def merge(buff1: AvgBuffer, buff2: AvgBuffer): AvgBuffer = {
    buff1.sum = buff1.sum + buff2.sum
    buff1.count = buff1.count + buff2.count
    buff1
  }

  // 计算
  override def finish(buff: AvgBuffer): Double = {
    buff.sum.toDouble / buff.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
