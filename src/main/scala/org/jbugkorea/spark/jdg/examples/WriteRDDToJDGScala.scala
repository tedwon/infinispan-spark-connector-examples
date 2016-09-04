package org.jbugkorea.spark.jdg.examples

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark._
import org.infinispan.spark.domain.Book

import scala.collection.JavaConverters._


/**
  * Good feature to share RDD.
  */
object WriteRDDToJDGScala {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("spark-infinispan-write-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD of Books
    val bookOne = new Book("Linux Bible", "desc", 2015, "Chris")
    val bookTwo = new Book("Java 8 in Action", "desc", 2014, "Brian")

    val sampleBookRDD = sc.parallelize(Seq(bookOne, bookTwo))
    val pairsRDD = sampleBookRDD.zipWithIndex().map(_.swap)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    infinispanProperties.put("infinispan.rdd.cacheName", "default")

    // Write the Key/Value RDD to the Data Grid
    pairsRDD.writeToInfinispan(infinispanProperties)


    // Debug cache data
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Long, Book] = cacheManager.getCache()
    println()
    cache.keySet().asScala
      .foreach(key => {
        val value = cache.get(key)
        println(s"key=$key value=$value")
      })

    // wait infinitely
    getClass synchronized {
      try
        getClass.wait()
      catch {
        case e: InterruptedException => {
        }
      }
    }

    sc.stop()
  }
}