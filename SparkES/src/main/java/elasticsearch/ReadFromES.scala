package elasticsearch

import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark._

/**
  * Created by Tan on 2017/1/23.
  */
object ReadFromES {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Write2ES").setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes","localhost")
    conf.set("es.net.http.auth.user","elastic")
    conf.set("es.net.http.auth.pass","changeme")
    val sc = new SparkContext(conf)

    val ESRDD = sc.esRDD("customer/external")
//    ESRDD.foreachPartition(println(_))
  }
}
