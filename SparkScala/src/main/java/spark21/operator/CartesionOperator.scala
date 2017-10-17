package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/7.
  */
object CartesionOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cartesion").setMaster("local")
    val sc = new SparkContext(conf)

    // 中文名 笛卡尔乘积
    // 比如说两个RDD，分部有10条数据，用了cartesian算子以后
    // 两个RDD的每一个数据都会和另外一个RDD的每一条数据执行一次JOIN
    // 最终组成一个笛卡尔乘积

    // 小案例
    // 比如说，现在有5件衣服，5条裤子，分部属于两个RDD
    // 就是说，需要对每件衣服都和每条裤子做一次JOIN操作，尝试进行服装搭配！
    val clothes = List("T恤衫","夹克","皮大衣","衬衫","毛衣")
    val trousers = List("西裤","内裤","铅笔裤","皮裤","牛仔裤")
    val clothesRDD = sc.parallelize(clothes)
    val trousersRDD = sc.parallelize(trousers)
    val pairs = clothesRDD.cartesian(trousersRDD)

    pairs.collect().foreach(println(_))
  }
}
