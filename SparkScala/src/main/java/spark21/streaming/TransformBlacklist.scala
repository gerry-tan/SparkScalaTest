package spark21.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Tan on 2017/2/15.
  */
object TransformBlacklist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformBlacklist").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))

    // 用户对我们网站上的广告可以进行点击,点击之后是不是要进行实时计费,但是对于刷广告的人,我们有一个黑名单
    // 只要是黑名单中的用户点击的广告,我们就给过滤掉

    // 先模拟一份黑名单RDD,true说明启用,false说明不启用
    val blacklist = Array(("yasaka", true),("xuruyun", false))

    val blacklistRDD = ssc.sparkContext.parallelize(blacklist)

    val adsClickLogDStream = ssc.socketTextStream("es-6",9999)

    // 所以要先对输入的数据进行转换操作变成 (username, date username) 以便后面对于数据流中的RDD和定义好的黑名单RDD进行join操作
    val userAdsClickLogDStream = adsClickLogDStream.map(acl => (acl.split(" ")(1),acl))

    // 实时进行黑名单过滤,执行transform操作,将每个batch的RDD,与黑名单RDD进行join
    val validAdsClickLogDStream = userAdsClickLogDStream.transform(
      rdd => {
        // 这里为什么是左外连接,因为并不是每个用户都在黑名单中,所以直接用join,那么没有在黑名单中的数据,无法join到就会丢弃
        // string是用户,string是日志,是否在黑名单里是Optional
        val joinedRDD = rdd.leftOuterJoin(blacklistRDD)
        val validRDD = joinedRDD.filter(tuple => {
          if (tuple._2._2.getOrElse(false)){
            false
          } else {
            true
          }
        })
        // 到此为止,filteredRDD中就只剩下没有被过滤的正常用户了,用map函数转换成我们要的格式,我们只要点击日志
        validRDD.map(_._2._1)
      }
    )

    // 这后面就可以写入Kafka中间件消息队列,作为广告计费服务的有效广告点击数据
    validAdsClickLogDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
