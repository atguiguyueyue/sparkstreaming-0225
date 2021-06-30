import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreaming02_RDDStream").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(6))

    //3.通过RDD队列创建DStream
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    // oneAtATime = true 默认，一次读取队列里面的一个RDD
    // oneAtATime = false， 按照设定的批次时间，读取队列里面数据
    val sourceDStream: InputDStream[Int] = ssc.queueStream(queue,false)

    //4.处理队列中的RDD数据
    val sumDStream = sourceDStream.reduce(_+_)

    //5.打印结果
    sumDStream.print()


    //开启任务并阻塞主线程
    ssc.start()

    //8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }

}
