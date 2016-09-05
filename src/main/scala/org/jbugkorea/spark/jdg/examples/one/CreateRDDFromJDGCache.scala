package org.jbugkorea.spark.jdg.examples.one

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.spark.domain.Book
import org.infinispan.spark.rdd.InfinispanRDD

/**
  * Get cache data and create RDD.
  * @see https://access.redhat.com/documentation/en-US/Red_Hat_JBoss_Data_Grid/7.0/html-single/Developer_Guide/index.html#Creating_and_USing_RDDs
  */
object CreateRDDFromJDGCache {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val cacheName = "book"

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    //
    // Populate sample data into cache
    //
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache = cacheManager.getCache[Integer, Book](cacheName)
    // Remove all data
    cache.clear()

    // Put some sample data to the remote cache
    // title and author
    val bookOne = new Book("Linux Bible", "Chris")
    val bookTwo = new Book("Java 8 in Action", "Brian")
    val bookThree = new Book("Spark", "Brian")
    cache.put(1, bookOne)
    cache.put(2, bookTwo)
    cache.put(3, bookThree)

    val cacheSize = cache.size()





    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    //
    // Start real example code from here
    //
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .setAppName("jdg-spark-connector-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // the connector's configurations
    // https://github.com/infinispan/infinispan-spark#supported-configurations
    val infinispanProperties = new Properties
    // List of servers
    infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
    // The name of the cache that will back the RDD
    infinispanProperties.put("infinispan.rdd.cacheName", cacheName)

    // Create RDD from cache
    // key: integer
    // value: book
    val infinispanRDD = new InfinispanRDD[Integer, Book](sc, configuration = infinispanProperties)

    // Debug RDD
    println()
    println("####################################")
    println("[Debug RDD]")

    infinispanRDD.foreach(println)

    println("####################################")
    println()


    val booksRDD: RDD[Book] = infinispanRDD.values

    val count = booksRDD.count()

    println()
    println("####################################")
    println(s"This book RDD has $count records.")
    println(s"$cacheName cache has $cacheSize entries.")
    println("####################################")
    println()







    // author's book count: word count
    // return RDD(count, author)
    val authors: RDD[(Int, String)] = infinispanRDD
      .map(x => x._2.getAuthor)
      .map(author => (author, 1))
      .reduceByKey(_ + _)
      .map(_.swap)

    //    authors.foreach(println)

    println()
    println("####################################")
    println("[Print RDD : author's book count]")

    authors.foreach(x => {
      val count = x._1
      val author = x._2
      println(s"Author=$author => Count=$count")
    })

    println("####################################")
    println()


    // wait infinitely
    getClass synchronized {
      try
        getClass.wait()
      catch {
        case e: InterruptedException => {
        }
      }
    }

  }
}
