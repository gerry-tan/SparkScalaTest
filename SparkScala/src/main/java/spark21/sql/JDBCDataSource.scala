package spark21.sql

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable

object JDBCDataSource {
  /*
    * ./bin/spark-submit --master local --class com.spark.study.sql.JDBCDataSource --driver-class-path ./lib/mysql-connector-java-5.1.32-bin.jar sparksql.jar
    */
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("dataframe").master("local[3]").getOrCreate()
    val options = new mutable.HashMap[String, String]
    options.put("url", "jdbc:mysql://localhost:3306/test?serverTimezone=UTC")
    options.put("user", "root")
    options.put("password", "root123")

    options.put("dbtable", "student_infos")
    val studentInfosDF: DataFrame = spark.read.format("jdbc").options(options).load
    options.put("dbtable", "student_scores")
    val studentScoresDF: DataFrame = spark.read.format("jdbc").options(options).load
    // 将两个DataFrame转换成JavePairRDD,进行join操作
    val studentsInfoRDD = studentInfosDF.rdd.map(x => (x.getString(0), x.getInt(1)))
    val studentsScoreRDD = studentScoresDF.rdd.map(x => (x.getString(0),x.getInt(1)))
    val studentsRDD = studentsInfoRDD.join(studentsScoreRDD)

    // 将JavaPairRDD转换为JavaRDD<Row>
    val studentRowsRDD: RDD[Row] = studentsRDD.map(x => Row(x._1,x._2._1,x._2._2))
    // 过滤
    val filteredStudentRowsRDD: RDD[Row] = studentRowsRDD.filter(_.getInt(2)>80)
    // 继续转换为DataFrame
    val structType: StructType = StructType(
        List(
          StructField("name", DataTypes.StringType),
          StructField("age", DataTypes.IntegerType),
          StructField("score", DataTypes.IntegerType)
        )
    )
    val studentsDF = spark.createDataFrame(filteredStudentRowsRDD, structType)
    val rows = studentsDF.collect
    for (row <- rows) {
      println(row)
    }
    // 将DataFrame数据保存到MySQL表中
    // 这种方式在公司里面是很常用的,有可能插入MySQL,有可能插入HBase,有可能插入Redis缓存
    studentsDF.foreach( row => {
      val sql = "insert into good_student_infos values(" + "'" + row.getString(0) + "'," + row.getInt(1) + "," + row.getInt(2) + ")"
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimezone=UTC&useUnicode=true&characterEncoding=UTF8", "root", "root123")
      val stat = conn.createStatement
      stat.executeUpdate(sql)

      if (stat != null) stat.close()
      if (conn != null) conn.close()
    })

    spark.stop()
  }
}

