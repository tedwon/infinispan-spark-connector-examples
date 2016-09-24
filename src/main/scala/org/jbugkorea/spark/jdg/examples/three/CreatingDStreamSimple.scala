package org.jbugkorea.spark.jdg.examples.three

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.event.ClientEvent.Type
import org.infinispan.spark.stream._

/**
  * Create a Spark DStream from cache-level events.
  * </p>
  * Insert, Modify and Delete event in a cache.
  * </p>
  * http://localhost:4040
  */
object CreatingDStreamSimple {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("jdg-spark-connector-example-DStream-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val configuration = new Properties
    configuration.put("infinispan.client.hotrod.server_list", infinispanHost)
    configuration.put("infinispan.rdd.cacheName", "stream")













    val ssc = new StreamingContext(sc, Seconds(1))

    // Create a Spark DStream from cache-level events
    val stream = new InfinispanInputDStream[Int, String](ssc, StorageLevel.MEMORY_ONLY, configuration)

//    stream.print()

//    val createdEventRDD: DStream[(Int, String)] = stream.filter { case (_, _, t) => t == org.infinispan.client.hotrod.event.ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED }
    val createdEventRDD: DStream[(Int, String)] = stream
      .map(x => {
        (x._1, x._2)
      })

    createdEventRDD.print()

    // Writing to JBoss Data Grid with DStreams
    // Use implicit method writeToInfinispan with JDG RDD configuration as input
    val writeConfiguration = new Properties
    writeConfiguration.put("infinispan.client.hotrod.server_list", infinispanHost)
    writeConfiguration.put("infinispan.rdd.cacheName", "default")
    createdEventRDD.writeToInfinispan(writeConfiguration)


    // Start the processing
    ssc.start()
    ssc.awaitTermination()
  }
}
