package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 计算数量的算子
  * Created by 谭谦 on 2016/8/7.
  */
object CountOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CountOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val numList =List(21,23,24,43,22)
    val numRDD = sc.parallelize(numList)
    val result = numRDD.count()
    println(result)

  }
}
