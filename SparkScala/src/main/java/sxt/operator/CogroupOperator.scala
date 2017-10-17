package sxt.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by 谭谦 on 2016/8/7.
  */
object CogroupOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Cogroup").setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = List(("1","xiaogao"),("2","xiaotao"),("3","xiaotan"))
    val scoreList = List(("1",100),("2",90),("3",90),("1",70),("2",80),("3",60))
    val students = sc.parallelize(studentList)
    val score = sc.parallelize(scoreList)

    // cogroup 与 join不同！
    // 相当于，一个key join上所有value，都给放到一个Iterable里面去！
    val result = students.cogroup(score)
    result.foreach(println(_))
//    result.foreach(x => {
//      println("id: "+x._1)
//      println("name: "+x._2._1)
//      println("score: "+x._2._2)
//    })
  }
}
