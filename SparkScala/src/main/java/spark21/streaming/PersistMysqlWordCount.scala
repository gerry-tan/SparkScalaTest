package spark21.streaming

import java.sql.{DriverManager, Statement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/1/20.
  */
object PersistMysqlWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val scc = new StreamingContext(conf, Seconds(5))
    scc.checkpoint("F:\\soft\\hadoop_wks\\SparkScalaTest\\resultfile\\checkpoint")
    val lines = scc.socketTextStream("es-6", 8888)
    val words = lines.flatMap(_.split(" "))
    val pairs: DStream[(String,Int)] = words.map((_, 1))
    val wordcounts = pairs.updateStateByKey[Int](
      (value: Seq[Int],state:Option[Int]) => {
        val newValue = value.sum
        val stateCount = state.getOrElse(0)
        Some(newValue + stateCount)
      }
    )
    // 每次得到当前所有单词的统计计数之后,讲其持久化以便于后续J2EE程序显示
    wordcounts.foreachRDD( wordcountsRDD => wordcountsRDD.foreachPartition(
      wordcount => {
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?serverTimezone=UTC&useUnicode=true&characterEncoding=UTF8", "root", "root123")
        var stat: Statement = null
        while (wordcount.hasNext) {
          val wc = wordcount.next()
          //mysql 中以word为键，当单词存在时更新count值，否则插入数据
          val sql = "insert into wordcount(word,count) "+ "values('" + wc._1 + "'," + wc._2 +
            ") ON DUPLICATE KEY UPDATE count="+wc._2
          stat = conn.createStatement
          stat.executeUpdate(sql)
        }
        if (stat != null) stat.close()
        if (conn != null) conn.close()
      }
    ))
    scc.start()
    scc.awaitTermination()
  }
}
