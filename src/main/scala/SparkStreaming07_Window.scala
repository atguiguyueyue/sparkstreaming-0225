import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Window {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Window")

    //TODO 1.创建sparkStreamingContext（上下文环境）
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //2.获取数据：从socket监听的9999端口中获取数据  (hello spark)
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.将传过来的数据按照空格切分，切分成一个一个单词  (hello) (spark)
    val wordsDStream: DStream[String] = socketDStream.flatMap(_.split(" "))

    //4.将数据转为Tuple元组（word，1）
    val wordToOneDStream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //TODO 5.开启一个窗口9s，滑动步长6s
    val windowDStream: DStream[(String, Int)] = wordToOneDStream.window(Seconds(9),Seconds(6))

    //5.做聚合计算
    val result: DStream[(String, Int)] = windowDStream.reduceByKey(_+_)

    //6.打印数据
    result.print()


    //TODO 7.开启任务（开启接收器接受数据）并阻塞main线程
    ssc.start()
    ssc.awaitTermination()
  }

}
