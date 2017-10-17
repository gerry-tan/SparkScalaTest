package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/15.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("es-6",8888)

    val pairs = lines.flatMap(_.split(" ")).map((_,1))

    val wordCount = pairs.reduceByKey(_+_)

    wordCount.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
