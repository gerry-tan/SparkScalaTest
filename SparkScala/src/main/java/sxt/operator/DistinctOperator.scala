package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 去重算子
  * Created by 谭谦 on 2016/8/7.
  */
object DistinctOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DistinctOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val nameRDD = sc.parallelize(List("xuruyun","tanggao","zhouzhentao","tanggao"))
    nameRDD.distinct().foreach(println(_))
  }
}
