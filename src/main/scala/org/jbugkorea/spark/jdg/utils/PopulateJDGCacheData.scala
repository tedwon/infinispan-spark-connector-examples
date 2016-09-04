package org.jbugkorea.spark.jdg.utils

import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.domain.Book


object PopulateJDGCacheData {

  def main(args: Array[String]): Unit = {
    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    val cache: RemoteCache[Integer, Book] = cacheManager.getCache("book")
    // Remove all data
    cache.clear()

    cache.put(1, new Book("Linux Bible", "desc", 2015, "Chris"))
    cache.put(2, new Book("Java 8 in Action", "desc", 2014, "Brian"))
    cache.put(3, new Book("Spark", "desc", 2014, "Brian"))

    println(cache.size())
  }
}
