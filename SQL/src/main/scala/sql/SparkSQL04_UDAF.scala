package sql

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//TODO 弱类型UDAF聚合函数，求年龄平均值
object SparkSQL04_UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.read.json("input/people.json")
    df.createOrReplaceTempView("user")

    //创建自定义聚合函数
    val udaf = new myAvgAge

    //注册聚合函数
    spark.udf.register("ageAvg", udaf)

    //使用聚合函数
    spark.sql("select ageAvg(age) from user").show
    spark.close()
  }
}

// TODO 继承UserDefinedAggregateFunction，重新八个方法
class myAvgAge extends UserDefinedAggregateFunction {

  // 聚合函数的输入数据结构，即参数
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 聚合函数的缓冲区数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 聚合函数的返回值类型
  override def dataType: DataType = DoubleType

  // 函数稳定性
  override def deterministic: Boolean = true

  // 缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 根据输入数据更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  // 合并缓冲区数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}