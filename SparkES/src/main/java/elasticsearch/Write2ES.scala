package elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
/**
  * Created by Tan on 2017/1/23.
  */
object Write2ES {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Write2ES").setMaster("local[1]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes","localhost")
    conf.set("es.net.http.auth.user","elastic")
    conf.set("es.net.http.auth.pass","changeme")
    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers,airports)).saveToEs("spark/docs")
  }
}
