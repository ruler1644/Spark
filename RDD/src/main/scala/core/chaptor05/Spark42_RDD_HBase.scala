package core.chaptor05

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark42_RDD_HBase {

  //TODO 读取HBase中数据
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()

    //hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "test")


    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    rdd.foreach {
      case (rowkey, result) => {
        for (cell <- result.rawCells()) {

          val str = Bytes.toString(CellUtil.cloneValue(cell))
          println(str)
        }

      }
    }

    sc.stop()
  }
}