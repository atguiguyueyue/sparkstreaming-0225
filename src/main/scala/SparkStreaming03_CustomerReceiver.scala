import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("custom").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.获取数据
    val socketDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102",9999))

    //4.将传过来的数据按照空格切分，切分成一个一个单词  (hello) (spark)
    val wordsDStream: DStream[String] = socketDStream.flatMap(_.split(" "))

    //5.将数据转为Tuple元组（word，1）
    val wordToOneDStream: DStream[(String, Int)] = wordsDStream.map((_,1))

    //6.做聚合计算
    val result: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

    //7.打印数据
    result.print()


    ssc.start()
    ssc.awaitTermination()
  }

  class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

    // receiver刚启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
    override def onStart() = {
      new Thread("socket name"){
        receiver()
      }.run()
    }

    /**
      * 用来获取端口数据并发送给spark
      */
    def receiver() ={
      //1.创建socket对象
      val socket: Socket = new Socket(host,port)

      //2.创建字符流
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

      var input: String = reader.readLine()
      while(!isStopped && input!=null){
        store(input)
        input= reader.readLine()
      }
      socket.close()
      reader.close()

      //重新启动接收器
//      restart("restart")


    }

    override def onStop(): Unit = {}
  }

}
