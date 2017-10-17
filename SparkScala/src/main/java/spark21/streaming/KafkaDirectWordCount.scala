package spark21.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/15.
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount")
    val ssc = new StreamingContext(conf,Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "spark001:9092,spark002:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaDirectWordCount",
      "auto.offset.reset" -> "earliest", //latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test","topicB")

    val lines = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String,String](topics,kafkaParams))

    val wordCount = lines.flatMap(_.value().split(":")).map((_,1)).reduceByKey(_+_)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
