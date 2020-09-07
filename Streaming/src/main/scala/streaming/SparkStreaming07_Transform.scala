package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Transform {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

        val ssc = new StreamingContext(sparkConf, Seconds(3))
        val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

        // Driver Coding(1)
        val transformDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {
            // Driver Coding(M)
            rdd.map(s=>{
                // Executor Coding(N)
                (s,1)
            })
        })

        // Driver Coding(1)
        val mapDStream: DStream[(String, Int)] = lineDStream.map(s=>{
            // Executor Coding(N)
            (s,1)
        })


        mapDStream.foreachRDD(rdd=>{
            rdd.foreach(println)
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
