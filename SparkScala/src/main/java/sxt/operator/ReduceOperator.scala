package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/8.
  */
object ReduceOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val numlist = List(1,2,3,4,5)
    val numbers = sc.parallelize(numlist)

    //reduce操作的原理：首先将第一个和第二个元素，传入call方法
    // 计算一个结果，接着把结果再和后面的元素依次累加
    // 以此类推
    val result = numbers.reduce(_+_)
    println(result)
  }
}
