import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transForm")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.从端口获取数据
    val sourceDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //4.使用transForm对数据做处理

    //TODO 执行一次，在dirver端
    println("transForm之外："+Thread.currentThread().getName)

    val resultDStream: DStream[(String, Int)] = sourceDStream.transform(rdd => {

      //TODO 每个批次执行一次，在dirver端
      println("transForm之内rdd算子之外："+Thread.currentThread().getName)

      val wordsRdd: RDD[String] = rdd.flatMap(_.split(" "))
      val result: RDD[(String, Int)] = wordsRdd.map(value => {
        //TODO 每条数据执行一次，在executor端
        println("rdd算子之内："+Thread.currentThread().getName)
        (value, 1)
      })
        .reduceByKey(_ + _)
      result
    })
    resultDStream.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
