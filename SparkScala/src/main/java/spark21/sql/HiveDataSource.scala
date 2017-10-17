package spark21.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Tan on 2017/1/23.
  */
object HiveDataSource {
  /*
    * 0,把hive里面的hive-site.xml放到spark/conf/目录下
    * 1,启动Hive
    * 2,您首先启动MySQL
    * 3,启动HDFS
    * 4,初始化HiveContext
    * 5,打包运行
    */
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("HiveDataSource2").master("local")
      .config("spark.sql.warehouse.dir","resultfile/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 判断是否存储过student_infos这张表，如果存储过则删除
    spark.sql("DROP TABLE IF EXISTS student_infos")
    // 重建
    spark.sql("CREATE TABLE IF NOT EXISTS student_infos ( name STRING , age INT )")
    // 加载数据，这里是HIVE的东西，我们主要是讲Spark SQL，所以HIVE的东西我们就不多言了
    spark.sql("LOAD DATA LOCAL INPATH 'F:/soft/hadoop_wks/SparkScala/student_infos.txt' INTO TABLE student_infos")
    // 一样的方式导入其它表
    spark.sql("DROP TABLE IF EXISTS student_scores")
    spark.sql("CREATE TABLE IF NOT EXISTS student_scores ( name STRING , score INT )")
    spark.sql("LOAD DATA LOCAL INPATH 'F:/soft/hadoop_wks/SparkScala/student_scores.txt' INTO TABLE student_scores")
    // 关联两张表，查询成绩大于80分的学生
    val goodStudentsDF = spark.sql("SELECT si.name, si.age, ss.score FROM student_infos si JOIN student_scores ss ON si.name=ss.name WHERE ss.score>=80")
    // 我们得到这个数据是不是还得存回HIVE表中！
    spark.sql("DROP TABLE IF EXISTS good_student_infos")
    goodStudentsDF.createOrReplaceTempView("good_student_infos")
    // 然后如果是一个HIVE表我们怎么给它读过来呢？
    val temp = spark.table("good_student_infos")
    val rows = temp.collect
    for (row <- rows) {
      System.out.println(row)
    }
    spark.close()
  }
}
