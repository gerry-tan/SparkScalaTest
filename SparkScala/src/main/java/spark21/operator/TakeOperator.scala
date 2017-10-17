package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/7.
  */
object TakeOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TakeOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val num = List(1,2,3,4,5,6)
    val numRDD = sc.parallelize(num)

    //取前三个
    val top3Num = numRDD.take(3)
    top3Num.foreach(println(_))
  }
}
