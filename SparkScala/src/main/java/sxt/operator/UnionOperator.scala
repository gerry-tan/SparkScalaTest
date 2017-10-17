package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/7.
  */
object UnionOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val name1 = List("xiaoming","xiaoqing","zhangsan","liuwu")
    val name2 = List("xiaoming","lisi","wangwu","zhaoliu")
    val nameRDD1 = sc.parallelize(name1)
    val nameRDD2 = sc.parallelize(name2)

    //逻辑的将两个patition连接在一起
    nameRDD1.union(nameRDD2).foreach(println(_))
  }
}
