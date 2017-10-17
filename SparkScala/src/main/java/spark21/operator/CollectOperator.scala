package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by 谭谦 on 2016/8/7.
  */
object CollectOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CollectOerator").setMaster("local")
    val sc = new SparkContext(conf)

    val list = List(1,2,3,4,5)
    val listRDD = sc.parallelize(list)
    val doubleList = listRDD.map(_*2)

    // 用foreach action操作，collect在远程集群上遍历RDD的元素
    // 用collect操作，讲分布式 的在远程集群里面的数据拉取到本地！！！
    // 这种方式不建议使用，如果数据量大，走大量的网络传输
    // 甚至有可能OOM内存溢出，通常情况下你会看到用foreach操作
    val collectList = doubleList.collect()
    collectList.foreach(println(_))
  }
}
