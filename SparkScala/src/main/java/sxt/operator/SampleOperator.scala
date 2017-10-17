package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/8.
  */
object SampleOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SampleOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val names = List("xurunyun", "liangyongqi", "wangfei","yasaka")
    val nameRDD = sc.parallelize(names)

    //false:不放回取样
    nameRDD.sample(false,0.33).foreach(println(_))
  }
}
