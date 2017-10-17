package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/1/20.
  */
object HDFSWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.textFileStream("hdfs://10.176.3.101:8020/orderrec/20170103")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))
    val wordcounts = pairs.reduceByKey(_+_)
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
