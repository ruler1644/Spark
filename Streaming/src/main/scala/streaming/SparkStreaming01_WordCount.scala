package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO 使用SparkStreaming完成WordCount操作
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dfgh")
    val ssc = new StreamingContext(conf, Seconds(3))

    //从指定端口获取一行数据
    //nc -lk 9999
    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //WordCount
    val wordDStream: DStream[String] = lineStream.flatMap(_.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val wordToSumStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    wordToSumStream.print()

    //启动采集器线程
    ssc.start()

    //阻塞当前Driver线程，等待采集器采集数据
    ssc.awaitTermination()
  }
}