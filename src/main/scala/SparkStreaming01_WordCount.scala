import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    //TODO 1.创建sparkStreamingContext（上下文环境）
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //2.获取数据：从socket监听的9999端口中获取数据  (hello spark)
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //3.将传过来的数据按照空格切分，切分成一个一个单词  (hello) (spark)
    val wordsDStream: DStream[String] = socketDStream.flatMap(_.split(" "))

    //4.将数据转为Tuple元组（word，1）
    val wordToOneDStream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //5.做聚合计算
    val result: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

    //6.打印数据
    result.print()
//    result.foreachRDD(rdd=>
//      rdd.foreach(value=>{
//        println(value)
//      })
//    )

    //TODO 7.开启任务（开启接收器接受数据）并阻塞main线程
    ssc.start()
    ssc.awaitTermination()
  }

}
