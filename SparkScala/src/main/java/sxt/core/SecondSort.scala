package sxt.core

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/9.
  */
object SecondSort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("sort.txt")
    val pair = lines.map(line => (new SecondSordKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))

    val sortpair = pair.sortBy(_._1,false)
    sortpair.map(_._2).foreach(println(_))
  }
}
