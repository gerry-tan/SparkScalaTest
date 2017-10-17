package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/7.
  */
object TakeSample {
  // takeSample = take + sample
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SampleOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val names = List("xuruyun", "liangyongqi", "wangfei","xuruyun")
    val namesRDD = sc.parallelize(names)

    //随机取样，false表示不放回取样，2表示取的数量
    val list = namesRDD.takeSample(false,2)
    list.foreach(println(_))
  }
}
