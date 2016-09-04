package org.jbugkorea.spark.jdg.examples

import java.util
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.query.RemoteQuery
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.client.hotrod.{RemoteCacheManager, Search}
import org.infinispan.protostream.FileDescriptorSource
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.infinispan.spark.domain.{Address, AddressMarshaller, Person, PersonMarshaller}
import org.infinispan.spark.rdd.InfinispanRDD

object FilteringRDDByQuery {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val protoFile =
      """
      package org.infinispan.spark.domain;
      message Person {
         required string name = 1;
         optional int32 age = 2;
         optional Address address = 3;
      }
      message Address {
         required string street = 1;
         required int32 number = 2;
         required string country = 3;
      }
      """.stripMargin

    // Obtain the remote cache
    lazy val cacheManager: RemoteCacheManager = {
      val rcm = new RemoteCacheManager(
        new ConfigurationBuilder().addServer().host("127.0.0.1").port(11222).marshaller(new ProtoStreamMarshaller).build()
      )
      rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME).put("test.proto", protoFile)

      val serCtx = ProtoStreamMarshaller.getSerializationContext(rcm)
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test.proto", protoFile))
      serCtx.registerMarshaller(new AddressMarshaller)
      serCtx.registerMarshaller(new PersonMarshaller)
      rcm
    }

    val protoConfig = {
      val map = new util.HashMap[String, String]()
      map.put("test.proto", protoFile)
      map
    }

    val defaultCache = cacheManager.getCache[Int, Person]
    defaultCache.clear()
    (1 to 20000000).foreach { idx =>
      defaultCache.put(idx, new Person(s"name$idx", idx, new Address(s"street$idx", idx, "N/A")));
      println(idx)
      Thread.sleep(100)
    }

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("spark-infinispan-example-filter-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val configuration = new Properties
    configuration.put("infinispan.rdd.query.proto.protofiles", protoConfig)
    configuration.put("infinispan.rdd.query.proto.marshallers", Seq(classOf[AddressMarshaller], classOf[PersonMarshaller]))
    configuration.put("infinispan.client.hotrod.server_list", infinispanHost)


    val rdd = new InfinispanRDD[Int, Person](sc, configuration)

    val query = Search.getQueryFactory(defaultCache)
      //      .from(classOf[Person]).having("address.number").gt(10)
      .from(classOf[Person]).having("name").like("name1%")
      .toBuilder[RemoteQuery].build()

    val filteredRdd = rdd.filterByQuery[Person](query, classOf[Person])

    filteredRdd.foreach(println)

    val filteredPersonRdd: RDD[Person] = filteredRdd.values

    val count = filteredPersonRdd.count()

    println(count)

  }
}
