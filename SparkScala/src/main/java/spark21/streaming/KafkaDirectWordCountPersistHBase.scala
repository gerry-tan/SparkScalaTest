package spark21.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import spark21.utils.{HBaseDAO, HBaseDAOImp}

/**
  * Created by Tan on 2017/2/15.
  */
object KafkaDirectWordCountPersistHBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectWordCountPersistHBase").setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "es-4:9092,es-5:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaDirectWordCount",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("topicA","topicB")

    val lines = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String,String](topics,kafkaParams))

    val wordCount = lines.flatMap(_.value().split(" ")).map((_,1)).reduceByKey(_+_)

    wordCount.print()

    wordCount.foreachRDD(wordCountRDD => {
      wordCountRDD.foreachPartition(wordCounts => {
        val dao: HBaseDAO = new HBaseDAOImp
        while (wordCounts.hasNext) {
          val wc = wordCounts.next()
          dao.insert("wordcount",wc._1+"_"+wc._2,"f1",Array[String](wc._1),Array[String](wc._2.toString))
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
