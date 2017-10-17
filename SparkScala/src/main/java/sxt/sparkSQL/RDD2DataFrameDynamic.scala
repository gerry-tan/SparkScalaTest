package sxt.sparkSQL

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by 谭谦 on 2016/8/9.
  */
object RDD2DataFrameDynamic {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD2DataFrameDynamic").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("students.txt")

    val schemaString = "id name age"
    //建立结构化元数据
    val schema = StructType(schemaString.split(" ").map(fieldName=> StructField(fieldName,StringType,true)))
//    val schema = StructType(Array(StructField("id",StringType,true),StructField("name",StringType,true),StructField("age",StringType,true)))
    //转换为RDD[Row]
    val rows = lines.map(_.split(",")).map(line => Row(line(0),line(1),line(2)) )
    //构建DataFrame
    val stuDF = sqlContext.createDataFrame(rows,schema)
    //将DataFrame注册为临时表并命名
    stuDF.registerTempTable("people")
//    stuDF.show()
    //打印元数据
//    stuDF.printSchema()
    //查询年龄小于等于18的学生
    val result = sqlContext.sql("select * from people where age<=18")
    result.show()
  }
}
