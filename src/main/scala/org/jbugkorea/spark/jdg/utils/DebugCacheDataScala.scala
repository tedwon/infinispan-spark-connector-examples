package org.jbugkorea.spark.jdg.utils

import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.domain.Book

import scala.collection.JavaConverters._


object DebugCacheDataScala {
  def main(args: Array[String]): Unit = {
    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, Book] = cacheManager.getCache("book")

    cache.keySet().asScala
      .foreach(key => {
        val value = cache.get(key)
        println(value)
      })
  }
}