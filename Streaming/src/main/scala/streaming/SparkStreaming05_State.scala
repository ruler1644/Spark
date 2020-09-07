package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//TODO 有状态操作UpdateStateByKey
object SparkStreaming05_State{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("h")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //将状态保存在checkpoint中
    ssc.sparkContext.setCheckpointDir("ck")

    //WordCount
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //更新状态，参数seq为当前批次单词频度，buffer为以往批次单词频度
    val stateDStream: DStream[(String, Long)] = wordToOneDStream.updateStateByKey[Long](
      (seq: Seq[Int], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    )
    stateDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
