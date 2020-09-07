package streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

//TODO 自定义采集器,从中获取数据
object SparkStreaming03_Source_DIY {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("dfghj")
    val ssc = new StreamingContext(conf, Seconds(5))

    //从自定义采集器中获取数据
    val myReceiver = new MySocketReceiver("hadoop102", 9999)
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(myReceiver)

    //WordCount
    val wordDStream: DStream[String] = lineStream.flatMap(line => line.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(s => (s, 1))
    val wordToSumStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    wordToSumStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

// TODO 自定义采集器,继承Receiver重写两个方法
class MySocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  def receive() = {

    //构建网络数据流
    val socket = new Socket(host, port)
    val inputStreamReader = new InputStreamReader(socket.getInputStream, "UTF-8")
    val bufferedReader = new BufferedReader(inputStreamReader)

    //读取数据
    var s: String = null
    Breaks.breakable {
      while ((s = bufferedReader.readLine()) != null) {
        if ("==END==".equals(s)) {
          Breaks.break()
        }

        //存储数据，自动构建DStream
        store(s)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread() {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {

  }
}