package spark21.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by 谭谦 on 2016/8/7.
  */
object JoinOperator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join").setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = List(("1","xiaogao"),("2","xiaotao"),("3","xiaotan"))
    val scoreList = List(("1",100),("2",90),("3",90),("1",70),("2",80),("3",60))
    val students = sc.parallelize(studentList)
    val score = sc.parallelize(scoreList)

    val result = students.join(score)
    result.sortBy(_._1).foreach(println(_))
    //    result.foreach(x => {
    //      println("id: "+x._1)
    //      println("name: "+x._2._1)
    //      println("score: "+x._2._2)
    //    })
  }
}
