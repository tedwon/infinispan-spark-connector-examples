package org.jbugkorea.spark.jdg.utils

import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.infinispan.spark.domain.Book


object PopulateJDGCacheDataScala {

  def main(args: Array[String]): Unit = {
    // Obtain the remote cache
    val cacheManager = new RemoteCacheManager
    //    val cache: RemoteCache[Integer, User] = cacheManager.getCache()
    //    cache.clear()
    //    (1 to 20000).foreach { idx =>
    //      cache.put(idx, new User(s"name$idx", idx))
    //      println(idx)
    //      Thread.sleep(100)
    //    }

    val cache: RemoteCache[Integer, Book] = cacheManager.getCache("book")
    cache.clear()
    cache.put(11, new Book("Linux Bible", "desc", 2015, "Chris"))
    cache.put(21, new Book("Java 8 in Action", "desc", 2014, "Brian"))
    cache.put(31, new Book("Spark", "desc", 2014, "Brian"))

    println(cache.size())

  }

}
