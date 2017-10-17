package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/13.
  */
object SocketStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("192.168.223.211", 9999)
    val words = lines.flatMap(_.split(" "))
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

//    val spark = SparkSession.builder().appName("SocketStreaming").master("local").getOrCreate()
//    val socketDF = spark.readStream.format("socket")
//      .option("host","192.168.223.211").option("port",9999).load()
//
//    //返回streaming dataframs类型的数据
//    socketDF.isStreaming
//    //打印数据
//    socketDF.printSchema()
//
//
//    val userScheme = new StructType().add("time","string").add("metric","string").add("value","integer")
//    val csvDF = spark.readStream.option("seq",",")
//      .schema(userScheme).csv("F:\\soft\\hadoop_wks\\SparkScalaTest\\SparkScala")
//      .where("value>1")
//
//    val query = csvDF.writeStream
//      .format("parquet").option("checkpointLocation","F:\\soft\\hadoop_wks\\SparkScalaTest\\resultfile\\checkpoint")
//      .start("F:\\soft\\hadoop_wks\\SparkScala\\SparkTest\\cpu.csv")
//
//    query.awaitTermination()
//
//    csvDF.show()
  }
}
