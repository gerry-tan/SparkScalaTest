package spark21.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Tan on 2017/2/15.
  */
object LoadSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoadSave").master("local").getOrCreate()

    val studentDF = spark.read.json("students.json")
//    val parquetFileDF = spark.read.parquet("student.parquet")

//    val student = parquetFileDF.createOrReplaceTempView("parquetFile")
//    val namesDF = spark.sql("select name from parquetFile where score>90")

//    namesDF.map(attritutes => "Name: " + attritutes(0)).show()
    studentDF.printSchema()
    studentDF.show()

//    studentDF.select("name","favorite_color").write.save("namesAndColors.parquet")
    spark.stop()
  }
}
