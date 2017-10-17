package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 排序
  * Created by root on 2016/8/7.
  */
object SortByKeyOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SortByKeyOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val score = List((150, "xuruyun"),(100, "liangyongqi"),(90, "wangfei"))
    val scores = sc.parallelize(score)

    //默认true按key的升序排列
    val result = scores.sortByKey(false)
    result.foreach(x=>println(x._2))
  }
}
