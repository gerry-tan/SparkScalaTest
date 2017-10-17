package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/15.
  */
object WindowBasedTopWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowBasedTopWord")
    val ssc = new StreamingContext(conf,Seconds(5))

    // 这里日志简化, yasaka hello, lily world,这里日志简化主要是学习怎么使用Spark Streaming的
    val searchLog = ssc.socketTextStream("es-6",8888)

    // 将搜索日志转换成只有一个搜索词即可
    val searchWordDstream = searchLog.map(_.split(" ")(1))

    // 将搜索词映射为(searchWord, 1)的Tuple格式
    val searchWordPairDstream = searchWordDstream.map((_,1))


    val searchWordCountsDstream = searchWordPairDstream.reduceByKeyAndWindow((a:Int,b:Int) => a+b,Seconds(60),Seconds(10))

    // 到这里就已经每隔10秒把之前60秒收集到的单词统计计数,12个小RDD,执行transform操作因为一个窗口60秒数据会变成一个RDD
    // 然后对这一个RDD根据每个搜索词出现频率进行排序然后获取排名前3热点搜索词,这里不用transform用transformToPair返回就是键值对
    val finalDstream = searchWordCountsDstream.transform(
      countSearchWordsRDD => {
        val sortedRDD = countSearchWordsRDD.map(tuple => (tuple._2,tuple._1)).sortByKey(false).map(tuple => (tuple._2,tuple._1))
        val topSearchWordCounts = sortedRDD.take(3)
//        for (wc <- topSearchWordCounts) {
//          println(wc._1+" "+wc._2)
//        }
        countSearchWordsRDD
      }
    )

    finalDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
