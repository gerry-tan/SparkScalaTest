package spark21.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Tan on 2017/2/15.
  */
object IntersectionOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IntersectionOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val names = Array("xurunyun", "liangyongqi", "wangfei", "yasaka")
    val names1 = Array("xurunyun", "liangyongqi2", "wangfei3", "yasaka4")

    val nameRDD = sc.parallelize(names,1)
    val nameRDD1 = sc.parallelize(names1,1)

    nameRDD.intersection(nameRDD1).foreach(println(_))
  }
}
