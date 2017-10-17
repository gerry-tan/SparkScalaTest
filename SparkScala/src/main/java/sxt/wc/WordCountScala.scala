package sxt.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 谭谦 on 2016/8/4.
  */
object WordCountScala {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)


//    sc.textFile("spark.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(-_._2).foreach(println(_))
    //以行读入
    val lines = sc.textFile("spark.txt")
    //扁平化map处理(map后将集合中的数据取出)
    val words = lines.flatMap(_.split(" "))
//    val words = lines.flatMap(line => line.split(" "))
    //将集合中的每个单词处理，设置为key-value元组
    val spair = words.map((_,1))
//    val spair = words.map(word => (word,1))
    //以元组的key分组，然后聚合处理，最后以value值降序排序
    val results = spair.reduceByKey(_+_).sortBy(-_._2)
//    val results = spair.reduceByKey((x,y) => x+y).sortBy(x => -x._2)
//    val results = spair.reduceByKey(_+_).map(tuple => (tuple._2,tuple._1)).sortByKey(false).map(tuple => (tuple._2,tuple._1))
    //循环打印results集合中的数据
    results.foreach(println(_))

    val distinctResult = words.distinct().map(_ => 1).reduce(_+_)

    println("distinctResult: "+distinctResult)

  }
}
