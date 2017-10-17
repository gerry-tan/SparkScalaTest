package spark21.test

import org.apache.spark.sql.SparkSession

/**
  * Created by Tan on 2017/2/13.
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaWordCount").master("local").getOrCreate()
    import spark.implicits._
    val ds1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers","host1:9092").option("subscribe","topic1").load()
    ds1.selectExpr("CAST(key as string),CAST(key as string)").as[(String,String)]

    //多个topic
    val ds2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers","host1:9092").option("subscribe","topic1,topic2").load()
    ds2.selectExpr("CAST(key as string),CAST(key as string)").as[(String,String)]

    //topic正则
    val ds3 = spark.readStream.format("kafka").option("kafka.bootstrap.servers","host1:9092").option("subscribe","topic.*").load()
    ds3.selectExpr("CAST(key as string),CAST(key as string)").as[(String,String)]
  }
}
