package spark21.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Tan on 2017/2/15.
  */
object DinstinctOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DinstinctOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val names = Array("xuruyun", "liangyongqi", "wangfei", "xuruyun")

    val nameRDD = sc.parallelize(names,1)

    nameRDD.distinct().foreach(println(_))
  }
}
