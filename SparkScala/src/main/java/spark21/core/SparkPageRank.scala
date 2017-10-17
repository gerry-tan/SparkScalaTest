/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark21.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object SparkPageRank {
  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      System.err.println("Usage: SparkPageRank <file> <iter>")
    //      System.exit(1)
    //    }
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[2]")
    val iters = 200
    //    val iters = if (args.length > 0) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile("page.txt")
    //设置checkpoint 的路径
    ctx.setCheckpointDir("E:\\soft\\hadoop_wks\\SparkScalaTest\\resultfile\\checkpoint")

    //根据边关系数据生成 邻接表 如：(1,(2,3,4,5)) (2,(1,5))..
    val links = lines.map(s =>(s.split("\\s+")(0), s.split("\\s+")(1))).distinct().groupByKey().cache()

    // (1,1.0) (2,1.0)..
    var ranks = links.mapValues(_ => 1.0)

    for (i <- 1 to iters) {
      // (1,((2,3,4,5), 1.0))
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      //迭代次数比较多，并且没有action算子提交job，可能导致内存不足，checkpoint将每一步rdd转换记录下来
      ranks.checkpoint()
    }

    ranks.collect().sortBy(_._2).foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    
    ctx.stop()
  }
}
