import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("foreachRdd").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //从端口获取数据
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //对读过来数据做wordCount
    val result = inputDStream
      .flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKey(_ + _)

    //TODO 将结果数据写入Redis
//    result.foreachRDD(rdd=>{
//      rdd.foreach(value=>{
//        //获取redis连接
//        val jedis: Jedis = new Jedis("hadoop102",6379)
//        //将数据写入redis
//        jedis.set(""+System.currentTimeMillis(),value._1)
//        //关闭连接
//        jedis.close()
//      })
//    })

    //TODO 优化：减少redis连接个数（每个分区下获取一次连接）
    result.foreachRDD(rdd=>{

      //dirver端一个批次执行一次
      println("位置1(foreachRDD里，算子外):"+Thread.currentThread().getName)
      rdd.foreachPartition(partition=>{

        //executor端一个分区执行一次
        println("位置2（foreachPartition算子中）:"+Thread.currentThread().getName)
        //获取redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)

        partition.foreach(vlaue=>{
          //executer端一条数据执行一次
          println("位置3:（foreach算子中）"+Thread.currentThread().getName)
          //        //将数据写入redis
                  jedis.set(""+System.currentTimeMillis(),vlaue)
        })
        //关闭连接
        jedis.close()
      })
    })


    result.print()



    ssc.start()
    ssc.awaitTermination()
  }

}
