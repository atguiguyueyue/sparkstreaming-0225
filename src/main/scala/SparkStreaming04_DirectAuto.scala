import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayOps

object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //设置Kafka相关参数，并封装到Map集合中
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "atguiguGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //3.通过direct模式获取kafka中的数据
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array("testTopic"), kafkaParams)
    )

    //4.统计单词个数
    val word: DStream[String] = kafkaDStream.map(record => {
      record.value()
    })
    //5.按照空格切分，切除每个单词
    val wordsDStream: DStream[String] = word.flatMap(_.split(" "))
    //6.将每个单词组成tuple元组
    val wordToOneDStream: DStream[(String, Int)] = wordsDStream.map((_, 1))
    //7.按照相同的单词，做聚合计算
    val result: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)
    //8.打印
    result.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
