package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 按键计算
  * 结果：Map(tanggao -> 1, xiaogao -> 2, xiaoming -> 1)
  * Created by root on 2016/8/7.
  */
object CountByKeyOperater {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CountByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val list = List(("tanggao",1),("xiaogao",3),("xiaogao",5),("xiaoming",9))
    val listRdd = sc.parallelize(list)
    val result = listRdd.countByKey()
    println(result)
  }
}
