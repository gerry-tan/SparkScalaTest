package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 将结果保存在文件
  * Created by 谭谦 on 2016/8/8.
  */
object SaveAsTextFileOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SaveAsTextFileOperator").setMaster("local")
    val sc = new SparkContext(conf)

    val result = sc.textFile("spark.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(-_._2)

    result.saveAsTextFile("hdfs://node3:8080/wc.txt")
  }
}
