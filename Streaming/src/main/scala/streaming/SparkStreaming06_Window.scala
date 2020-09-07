package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//TODO 窗口操作：采集多个周期的数据汇总成窗口数据
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("h")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //采集周期和滑动步长，都是窗口长度的整数倍
    val windowDStream: DStream[String] = lineDStream.window(Seconds(9), Seconds(3))

    //WordCount
    val wordDStream: DStream[String] = windowDStream.flatMap(_.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    wordToSumDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
