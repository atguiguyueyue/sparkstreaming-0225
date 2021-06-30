import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_ReduceByKeyAndWindow_Reduce_ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduce")

    //TODO 1.创建sparkStreamingContext（上下文环境）
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("D:\\MyWork\\hadoop\\WorkSpeace\\IdeaSpeace\\sparkstreaming-0225\\output")

    //2.获取数据：从socket监听的9999端口中获取数据  (hello spark)
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.将传过来的数据按照空格切分，切分成一个一个单词  (hello) (spark)
    val wordsDStream: DStream[String] = socketDStream.flatMap(_.split(" "))

    //4.将数据转为Tuple元组（word，1）
    val wordToOneDStream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //TODO 5.开启一个reduceByKeyAndWindow窗口9s，滑动步长6s
    val result: DStream[(String, Int)] = wordToOneDStream.reduceByKeyAndWindow(
      (a: Int, b: Int) => (a + b),
      (x:Int,y:Int)=>(x-y),
      Seconds(9),
      Seconds(6),
      new HashPartitioner(2),
      //通过这个过滤的匿名函数，将value为0的情况过滤掉，不打印
      (x:(String, Int)) => x._2 > 0
    )

    //6.打印数据
    result.print()


    //TODO 7.开启任务（开启接收器接受数据）并阻塞main线程
    ssc.start()
    ssc.awaitTermination()
  }

}
