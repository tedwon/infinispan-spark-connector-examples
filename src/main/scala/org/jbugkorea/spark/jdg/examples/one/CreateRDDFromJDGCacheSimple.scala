package org.jbugkorea.spark.jdg.examples.one

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark._
import org.infinispan.spark.domain.Book
import org.infinispan.spark.rdd.InfinispanRDD

/**
  * @see https://access.redhat.com/documentation/en-US/Red_Hat_JBoss_Data_Grid/7.0/html-single/Developer_Guide/index.html#Creating_and_USing_RDDs
  */
object CreateRDDFromJDGCacheSimple {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
//    Logger.getLogger("org.infinispan.spark").setLevel(Level.TRACE)

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
    val cache: RemoteCache[Integer, Book] = cacheManager.getCache(cacheName)
    // Remove all data
    cache.clear()

    cache.put(1, new Book("Linux", "Chris"))
    cache.put(2, new Book("Java", "Brian"))
    cache.put(3, new Book("Spark", "Brian"))








    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    //
    // Start real example code from here
    //
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    val conf = new SparkConf()
      .setAppName("jdg-spark-connector-example-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", "127.0.0.1:11222;127.0.0.1:11372")
    infinispanProperties.put("infinispan.rdd.cacheName", cacheName)

    // Create RDD from cache
    // key: integer
    // value: book pojo object
    val infinispanRDD = new InfinispanRDD[Integer, Book](sc, configuration = infinispanProperties)

    // Print result all author and title
    println()
    println("####################################")
    println("[Print RDD : all books]")

    infinispanRDD.foreach(x => {
      val author = x._2.getAuthor
      val title = x._2.getTitle
      println(s"AUTHOR=$author, TITLE=$title")
    })

    println("####################################")
    println()



    // author's book count: word count
    val authors: RDD[(Int, String)] = infinispanRDD.map(x => x._2.getAuthor)
      .map(author => (author, 1))
      .reduceByKey(_ + _).map(_.swap)

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

    // Write the Key/Value RDD to the Data Grid
    // authors: RDD[(Int, String)]
    infinispanProperties.put("infinispan.rdd.cacheName", "author")
    authors.writeToInfinispan(infinispanProperties)

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
