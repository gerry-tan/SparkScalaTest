package elasticsearch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

/**
  * Created by Tan on 2017/1/23.
  */
object sqlForES {
  def main(args: Array[String]): Unit = {
    //config配置：https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    val spark = SparkSession.builder()
      .config("es.index.auto.create", "true")
      //.config("es.resource","spark/students")
      .config("es.nodes","localhost")
      .config("es.port","9200")
      .config("es.net.http.auth.user","elastic")
      .config("es.net.http.auth.pass","changeme")
      .appName("sqlForES").master("local")
      .getOrCreate()

    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._
    //  create DataFrame
    val student: DataFrame = spark.sparkContext.textFile("students.txt")
      .map(_.split(","))
      .map(p => Student(p(0).trim.toInt, p(1), p(2).trim.toInt))
      .toDF()
    student.show()
    student.saveToEs("spark/student")
  }
}

// case class used to define the DataFrame
case class Student(id: Int, name: String,  age: Int)
