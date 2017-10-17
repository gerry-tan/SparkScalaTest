package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2016/8/8.
  */
object RepartitionOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RepartitionOperator").setMaster("local")
    val sc = new SparkContext(conf)
    // repartition算子，用于任意将RDD的partition增多或者减少！
    // coalesce仅仅将RDD的partition减少！
    // 建议使用的场景
    // 一个很经典的场景，使用Spark SQL从HIVE中查询数据时候，spark SQL会根据HIVE
    // 对应的hdfs文件的block的数量决定加载出来的RDD的partition有多少个！
    // 这里默认的partition的数量是我们根本无法设置的

    // 有些时候，可能它会自动设置的partition的数量过于少了，为了进行优化
    // 可以提高并行度，就是对RDD使用repartition算子！

    // 公司要增加部门

    val staffList = List("xuruyun1","xuruyun2","xuruyun3","xuruyun4","xuruyun5","xuruyun6"
      ,"xuruyun7","xuruyun8","xuruyun9","xuruyun10","xuruyun11","xuruyun12")

    val staffRDD = sc.parallelize(staffList,3)
    val staffRDD2 = staffRDD.mapPartitionsWithIndex((index,iter)=>{
      var list = List("")
      while (iter.hasNext) {
        if (list.tail == ""){
          list = list.drop(1)
        }
        val i = "部门[" + (index+1) + "]:" + iter.next()
        list = (i)::list
      }
      list.iterator
    })
    staffRDD2.foreach(println(_))

    val staffRDD3 = staffRDD2.repartition(6)
    val staffRDD4 = staffRDD3.mapPartitionsWithIndex((index,iter)=>{
      var list = List("")
      while (iter.hasNext ) {
        if (list.tail == ""){
          list = list.drop(1)
        }
        val i = "部门[" + (index+1) + "]:" + iter.next()
        list = (i)::list
      }
      list.iterator
    })
    staffRDD4.foreach(println(_))

  }
}
