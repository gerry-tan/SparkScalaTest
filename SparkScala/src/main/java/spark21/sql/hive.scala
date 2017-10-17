package spark21.sql

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Tan on 2017/1/23.
  */
object hive {
  def main(args: Array[String]): Unit = {
    case class Record(name: String, age: Int)

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "resultfile/spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example").master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE TABLE IF NOT EXISTS src (name string, age int)")
    spark.sql("LOAD DATA LOCAL INPATH 'F:\\soft\\hadoop_wks\\SparkScalaTest\\student_infos.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show()

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM src").show()

    val sqlDF = spark.sql("SELECT name, age FROM src WHERE age > 22 ORDER BY age")

    val stringsDS = sqlDF.map {
      case Row(name: String, age: Int) => s"name: $name, age: $age"
    }
    stringsDS.show()
    spark.sql("SELECT * FROM records r JOIN src s ON r.name = s.name").show()

    spark.stop()
  }
}
