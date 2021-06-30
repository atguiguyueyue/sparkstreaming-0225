import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object sparkStreaming06_updateStateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming06_WordCount")

    //TODO 1.创建sparkStreamingContext（上下文环境）
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //使用updateStateByKey必须要设置检查点目录
    ssc.checkpoint("D:\\MyWork\\hadoop\\WorkSpeace\\IdeaSpeace\\sparkstreaming-0225\\output")


    //2.获取数据：从socket监听的9999端口中获取数据  (hello spark)
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //3.将传过来的数据按照空格切分，切分成一个一个单词  (hello) (spark)
    val wordsDStream: DStream[String] = socketDStream.flatMap(_.split(" "))

    //4.将数据转为Tuple元组（word，1）
    val wordToOneDStream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //5 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val result: DStream[(String, Int)] = wordToOneDStream.updateStateByKey(
      fun
    )

    //6.打印数据
    result.print()


    //TODO 7.开启任务（开启接收器接受数据）并阻塞main线程
    ssc.start()
    ssc.awaitTermination()
  }
 def fun= (seq:Seq[Int], opt:Option[Int]) => {
    //1.获取当前批次的结果
    val currentSum: Int = seq.sum
    //2.获取历史的结果
    val historySum: Int = opt.getOrElse(0)
    Some(currentSum+historySum)
  }
}
