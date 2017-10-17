package sxt.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by 谭谦 on 2016/8/9.
  */
object RDD2DataFrameReflection {

  case class Student(id:Int,name:String,age:Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext  = new SQLContext(sc)

    import sqlContext.implicits._

    val studentsDF = sc.textFile("students.txt").map(line => Student(line.split(",")(0).toInt,line.split(",")(1),line.split(",")(2).toInt)).toDF()
    studentsDF.registerTempTable("people")

    studentsDF.show()

    val results1 = sqlContext.sql("select * from people where age<=18")
    results1.show()
    val stu = results1.rdd.map(row => Student(row.get(1).toString.toInt,row.get(2).toString,row.get(0).toString.toInt))
    results1.collect().foreach(println(_))
  }
}
