package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by 谭谦 on 2016/8/7.
  */
object CoalesceOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Coalesce").setMaster("local")
    val sc = new SparkContext(conf)

    // coalesce算子，功能是将RDD的partition的数量缩减，减少！！！
    // 将一定的数据压缩到更少的partition分区中去
    // 使用场景！很多时候在filter算子应用之后会优化一下使用coalesce算子
    // filter 算子应用到RDD上面，说白了会应用到RDD对应的里面的每个partition上去
    // 数据倾斜，换句话说就是有可能有的partition里面就剩下了一条数据！
    // 建议使用coalesce算子，从而让各个partition中的数据都更加的紧凑！！

    // 公司原先有6个部门
    // 但是呢，可不巧，碰到了公司的裁员！裁员以后呢，有的部门中的人员就没有了！
    // 数据倾斜，不同的部门人员不均匀，做一个部门的整合的操作，将不同的部门的员工进行压缩！

    val staffList = List("xiaogao1","xiaogao2","xiaogao3","xiaogao4","xiaogao5","xiaogao6","xiaogao7","xiaogao8","xiaogao9","xiaogao10","xiaogao11","xiaogao12")
    val staffRDD = sc.parallelize(staffList,6)
    val staffRDD2 = staffRDD.mapPartitionsWithIndex((index,iter)=>{
      var list = List[String]()
      var i = ""
      while (iter.hasNext) {
        i += iter.next()
      }
      list.::(index+":"+i).iterator
    },true)

    staffRDD2.foreach(println(_))
  }
}
