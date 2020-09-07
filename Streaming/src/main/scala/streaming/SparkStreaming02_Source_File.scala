package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

//TODO 从指定文件路径中获取数据
object SparkStreaming02_Source_File {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("d")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 从指定文件路径中获取数据
    val lineStream: DStream[String] = ssc.textFileStream("in")

    //WordCount
    val wordDStream: DStream[String] = lineStream.flatMap(line => line.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(s => (s, 1))
    val wordToSumStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    wordToSumStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
