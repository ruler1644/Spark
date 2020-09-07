package core.chaptor04

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{Dependency, SparkConf, SparkContext}

object Spark40_RDD_MySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/tmp"
    val username = "root"
    val password = "root"
    val sql = "select * from stu where id >=? and id <=?"

    //TODO 从MySQL中获取数据，必须有上下边界，用于分布式并行计算
    val jdbc = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      },
      sql,
      1,
      3,
      3,
      (rs) => {

        //Executor端执行打印操作
        println(rs.getObject(1) + ","
          + rs.getObject(2) + ","
          + rs.getObject(3))

      }
    )

    println(jdbc.count())
    sc.stop()

  }
}