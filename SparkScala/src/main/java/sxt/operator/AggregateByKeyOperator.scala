package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/7.
  */
object AggregateByKeyOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AggregateByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("spark.txt")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_,1))

    val result = pairs.aggregateByKey(0)((v1,v2)=>{v1+v2},(v1,v2)=>{v1+v2})
    result.collect().foreach(println(_))
  }
}
