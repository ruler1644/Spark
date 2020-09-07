package core.chaptor05

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapred.{TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark43_RDD_HBase {

  //TODO 向HBase写入数据
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("haha")
    val sc = new SparkContext(conf)

    //创建HBaseConf
    val hbaseConf = HBaseConfiguration.create()
    //hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "test")

    val rdd: RDD[(String, String)] = sc.makeRDD(List(("1003", "tom"), ("1004", "jack")))
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map {
      case (rowkey, data) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(data))
        (new ImmutableBytesWritable, put)
      }
    }


    //创建JobConf,连接HBase，向test表插入数据
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")

    //保存数据
    putRDD.saveAsHadoopDataset(jobConf)


    sc.stop()

  }
}