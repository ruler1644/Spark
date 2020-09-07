package core.chaptor04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，涉及线程之间的通信
object Spark34_RDD_Ser {
  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //含有字符"h"的单词
    val search = new Search("h")

    /*//TODO 闭包检测
    rdd.foreach(s =>
      println(search)
    )*/


    //hadoop
    //hive
    val rdd1: RDD[String] = rdd.filter(str => str.contains("h"))

    //Caused by: java.io.NotSerializableException: Search
    val resRDD1: RDD[String] = search.getMatch1(rdd)
    resRDD1.collect().foreach(println)

    //Caused by: java.io.NotSerializableException: com.atguigu.Search
    val resRDD2: RDD[String] = search.getMatch2(rdd)
    resRDD2.collect().foreach(println)
    sc.stop()


    //TODO 闭包
    def fun1(i: Int) = {
      def fun2(j: Int) = {
        i + j
      }

      //fun2
      fun2 _
    }

    //<function1>
    //22
    println(fun1(10))
    println(fun1(10)(12))
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)


    //TODO 3.有序列化时，Nil正确运行，不报错
    //TODO 4.没有序列化时，Nil报错
    //val rdd: RDD[String] = sc.parallelize(Nil)

    //含有字符"h"的单词
    val search = new Search("h")


    //TODO 1.不涉及到对象的序列化，可以打印出包含"h"的字符，hadoop ive
    //    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    //    val rdd1: RDD[String] = rdd.filter(str => str.contains("h"))
    //    rdd1.collect().foreach(println)


    //TODO 2.如果对象没有序列化，会产生异常。Caused by: java.io.NotSerializableException: Search
    //在Driver端执行不需要序列化，在Executor端执行时，需要从Driver端传递，需要序列化
    //    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    //    val resRDD1: RDD[String] = search.getMatch1(rdd)
    //    resRDD1.collect().foreach(println)

    //TODO 5.闭包检测，没有执行Task任务，就已经检测出对象没有序列化
    //foreach函数用到外部对象search，存在闭包
    //    rdd.foreach(s => {
    //      println(search)
    //    })

    //TODO 6.闭包：把函数外的第三方变量，在运行过程中，包含到函数内部，形成一个闭合的效果，可以改变变量的生命周期
    //函数执行时要压栈，方法的参数，就是方法的本地变量
    //i是函数fun1的本地变量，当fun1执行完毕出栈后，本地变量i应该回收
    def fun1(i: Int) = {
      def fun2(j: Int) = {
        i + j
      }

      //将函数fun2返回
      fun2 _
    }

    //<function1>
    //30
    println(fun1(10))
    println(fun1(10)(20))


    //TODO 7.Caused by: java.io.NotSerializableException: com.atguigu.Search
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    val resRDD2: RDD[String] = search.getMatch2(rdd)
    resRDD2.collect().foreach(println)
    sc.stop()

  }

}


//class Search(query: String) {
class Search(query: String) extends Serializable {

  def isMatch(s: String) = {
    s.contains(query)
  }

  //在这个方法中所调用的方法isMatch()是定义在Search这个类中的，
  //实际上调用的是this.isMatch()， this表示Search这个类的对象，
  //程序在运行过程中，需要将Search对象序列化以后传递到Executor端
  def getMatch1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  //在这个方法中，匿名函数参数query是定义在Search这个类中的字段，
  //实际上调用的是this.query，this表示Search这个类的对象，
  //程序在运行过程中需要将Search对象序列化以后传递到Executor端
  def getMatch2(rdd: RDD[String]) = {
    rdd.filter(s => s.contains(query))
  }

  //不序列化也可以正确执行，把构造参数变成了函数内部的普通局部变量，可以网络传输，不需要序列化
  def getMatch3(rdd: RDD[String]) = {
    val q = query
    rdd.filter(s => s.contains(q))
  }
}