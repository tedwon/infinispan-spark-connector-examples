package org.jbugkorea.spark.jdg.examples

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark._
import org.infinispan.spark.domain.Book
import org.infinispan.spark.rdd.InfinispanRDD

object CreateRDDFromJDGCacheSimple {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org.infinispan.spark").setLevel(Level.TRACE)

    val conf = new SparkConf()
      .setAppName("spark-infinispan-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", "127.0.0.1:11222;127.0.0.1:11372")
    infinispanProperties.put("infinispan.rdd.cacheName", "book")

    // Create RDD from cache
    val infinispanRDD = new InfinispanRDD[Integer, Book](sc, configuration = infinispanProperties)

    // Debug result
    infinispanRDD.foreach(x => {
      val author = x._2.getAuthor
      val title = x._2.getTitle
      println(s"author=$author title=$title")
    })

    // author word count
    val authors = infinispanRDD.map(x => x._2.getAuthor)
      .map(author => (author, 1))
      .reduceByKey(_ + _).map(_.swap)

    authors.foreach(println)

    // Write the Key/Value RDD to the Data Grid
    infinispanProperties.put("infinispan.rdd.cacheName", "author")
    authors.writeToInfinispan(infinispanProperties)

    // wait infinitely
    //    getClass synchronized {
    //      try
    //        getClass.wait()
    //      catch {
    //        case e: Exception => {
    //        }
    //      }
    //    }

    sc.stop()
  }
}
