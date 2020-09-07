package streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Source_Kafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dfgh")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO 从kafka中采集数据(旧版本连接Zookeeper)，createStream方法的四个参数
    /* ssc: StreamingContext,
       kafkaParams: Map[String, String],
       topics: Map[String, Int],
       storageLevel: StorageLevel
    */

    //通过KafkaUtil创建kafkaDStream
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map(
        ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
        "zookeeper.connect" -> "hadoop102:2181",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
      ),
      Map("topic0408" -> 3),
      StorageLevel.MEMORY_ONLY
    )

    //WordCount
    //获取的kafka数据是topic-value对，对数据进行处理
    val lineDStream: DStream[String] = kafkaDStream.map(_._2)
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    val wordToSumStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    wordToSumStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
