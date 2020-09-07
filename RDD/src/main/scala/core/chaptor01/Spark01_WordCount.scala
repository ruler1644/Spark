package core.chaptor01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//第一个spark程序，WordCount
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConfig并设置app名称
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    // 读取数据文件(以项目的根目录为基准)
    // 使用sc创建RDD并执行相应的transformation和action
    val lines: RDD[String] = sc.textFile("input/words.txt", 2)

    //获取一行数据，并进行切分(一步步化简)
    /*
        //定义函数
        def myFlatMap(str: String): Array[String] = {
          str.split(" ")
        }
        def myFlatMap2(str: String): Array[String] = str.split(" ")
        def myFlatMap3(str: String) = str.split(" ")

        //函数作为参数传递给另一个函数
        lines.flatMap(myFlatMap)
        lines.flatMap((s: String) => s.split(" "))
        lines.flatMap((s) => s.split(" "))
        lines.flatMap( s => s.split(" "))
        lines.flatMap(_.split(" "))

    */

    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将数据进行结构的转换，hello --->(hello,1)
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    //将转换结构后的数据，进行分组(ByKey)聚合
    //下划线_可以看作占位符，参数出现一次时，可用_替代
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //将结果采集完毕，展现在控制台上
    val res: Array[(String, Int)] = wordToSum.collect()

    println(res.mkString(","))

    //关闭连接
    sc.stop()
  }
}
