package org.jbugkorea.spark.jdg.examples

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.domain.Book
import org.infinispan.spark.rdd.InfinispanRDD

object CreateRDDFromJDGCache {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, Book] = cacheManager.getCache()

    // Put some sample data to the remote cache
    val bookOne = new Book("Linux Bible", "desc", 2015, "Chris")
    val bookTwo = new Book("Java 8 in Action", "desc", 2014, "Brian")
    val bookThree = new Book("Spark", "desc", 2014, "Brian")
    cache.put(1, bookOne)
    cache.put(2, bookTwo)
    cache.put(2, bookThree)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("spark-infinispan-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    infinispanProperties.put("infinispan.rdd.cacheName", "default")

    // Create RDD from cache
    val infinispanRDD = new InfinispanRDD[Integer, Book](sc, configuration = infinispanProperties)

    infinispanRDD.foreach(println)

    val booksRDD: RDD[Book] = infinispanRDD.values

    val count = booksRDD.count()

    println(count)
  }
}
