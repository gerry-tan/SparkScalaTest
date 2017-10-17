package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/15.
  */
object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
    val ssc = new StreamingContext(conf,Seconds(5))

    // 第一点,如果要使用updateStateByKey算子,就必须设置一个checkpoint目录,开启checkpoint机制
    ssc.checkpoint("E:\\soft\\hadoop_wks\\SparkScalaTest\\resultfile\\checkpoint")

    val words = ssc.socketTextStream("es-6",9999).flatMap(_.split(" "))

    val pairs = words.map((_,1))

    val wordcount = pairs.updateStateByKey(
      (value: Seq[Int],state:Option[Int]) => {
        val newValue = value.sum
        val stateCount = state.getOrElse(0)
        Some(newValue + stateCount)
      }
    )

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
