package org.jbugkorea.spark.jdg.examples

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.domain.User
import org.infinispan.spark.stream._


object CreatingDStreamSimple {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("jdg-spark-connector-example-filter-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val configuration = new Properties
    configuration.put("infinispan.client.hotrod.server_list", infinispanHost)
    configuration.put("infinispan.rdd.cacheName", "stream")

    val ssc = new StreamingContext(sc, Seconds(1))

    val stream = new InfinispanInputDStream[Int, String](ssc, StorageLevel.MEMORY_ONLY, configuration)

//    stream.print()

    val createdEventRDD = stream.filter { case (_, _, t) => t == org.infinispan.client.hotrod.event.ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED }

    createdEventRDD.print()



    // Start the processing
    ssc.start()
    ssc.awaitTermination()
  }
}
