package spark21.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 2016/8/9.
  */
object SecondSort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("sort.txt")

//    lines.map(line => (line.split(" ")(0).toInt,line.split(" ")(1).toInt))
//      .aggregateByKey(ArrayBuffer[Int]())(_:+_,_++_)
//        .mapValues(_.sortBy(-_)).sortByKey(false).foreach(println(_))

    lines.map(line => (line.split(" ")(0).toInt,line.split(" ")(1).toInt))
      .groupByKey().sortBy(_._2).sortByKey(false)
      .map(x => {
        val key = x._1
        val values = x._2
        for (v <- values) {
          println((key,v))
          (key,v)
        }
      })
      .foreach(println(_))

//    lines.map(line => (new SecondSordKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))
//          .sortBy(_._1,false).map(_._2).foreach(println(_))
  }
}
