package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/15.
  */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    // ./bin/kafka-topics.sh --zookeeper spark001:2181,spark002:2181,spark003:2181 --topic wordcount --replication-factor 1 --partitions 1 --create
    // ./bin/kafka-console-producer.sh --topic wordcount --broker-list spark001:9092,spark002:9092,spark003:9092
    // 这个比较重要,是对应你给topic用几个线程去拉取数据
    val topicThreadMap = Map("20170215" -> 1)

    val zkQuorum = "es-4:2181,es-5:2181,es-6:2181"
    val groupId = "KafkaReceiverWordCount"

    // kafka这种创建的流,是pair的形式,有俩个值,但第一个值通常都是Null啊
//    val lines = KafkaUtils.createStream(ssc,zkQuorum,groupId,topicThreadMap)
//
//    val wordCount = lines.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_)
//
//    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
