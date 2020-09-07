package core.chaptor05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark41_RDD_MySQL {

  //TODO 将数据写入MySQL
  //TODO 使用foreach每条数据都会建立一次连接，频繁建立释放连接，效率太差了
  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/tmp"
    val username = "root"
    val password = "root"

    val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((4, "jack", 28), (5, "pony", 29), (6, "robin", 30)))

    //数据循环遍历不需要需要返回值，使用foreach。若需要返回值使用map
    dataRDD.foreach {
      case (id, name, age) => {
        Class.forName(driver)
        val ct = DriverManager.getConnection(url, username, password)
        val sql = "insert into stu(id, name, age) values(?,?,?)"
        val ps = ct.prepareStatement(sql)
        ps.setObject(1, id)
        ps.setObject(2, name)
        ps.setObject(3, age)
        ps.executeUpdate()
        ps.close()
        ct.close()
      }
    }
    sc.stop()
  }


  //TODO 代码错误，连接对象无法序列化
  def main2(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/tmp"
    val username = "root"
    val password = "root"

    val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((4, "jack", 28), (5, "pony", 29), (6, "robin", 30)))

    Class.forName(driver)
    val ct = DriverManager.getConnection(url, username, password)
    val sql = "insert into stu(id, name, age) values(?,?,?)"

    val ps = ct.prepareStatement(sql)
    dataRDD.foreach {
      case (id, name, age) => {

        //TODO java.io.NotSerializableException: com.mysql.jdbc.JDBC4PreparedStatement
        //算子内部用到外部的连接对象，形成闭包，闭包检测时，发现对象没有序列化
        //但是连接对象无法序列化，引出foreachPartition
        ps.setObject(1, id)
        ps.setObject(2, name)
        ps.setObject(3, age)
        ps.executeUpdate()
      }
    }
    ps.close()
    ct.close()
    sc.stop()
  }

  //TODO  使用foreachPartition提高效率
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/tmp"
    val username = "root"
    val password = "root"

    val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((4, "jack", 28), (5, "pony", 29), (6, "robin", 30)))
    dataRDD.foreachPartition(datas => {

      //每个分区的数据获取一次连接，是foreachPartition算子内部代码，在Executor端执行，不需要序列化连接对象
      Class.forName(driver)
      val ct = DriverManager.getConnection(url, username, password)
      val sql = "insert into stu(id, name, age) values(?,?,?)"
      val ps = ct.prepareStatement(sql)

      //foreach方法，在Executor内存中完成
      datas.foreach {
        case (id, name, age) => {
          ps.setObject(1, id)
          ps.setObject(2, name)
          ps.setObject(3, age)
          ps.executeUpdate()
        }
      }
      ps.close()
      ct.close()
    })

    sc.stop()
  }
}