package org.jbugkorea.spark.jdg.examples.five

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

/**
  * Use JDG server side filters to create a cache based RDD.</p>
  * Using Infinispan Query DSL
  * @see https://access.redhat.com/documentation/en-US/Red_Hat_JBoss_Data_Grid/7.0/html-single/Developer_Guide/index.html#Using_the_Infinispan_Query_DSL_with_Spark
  */
object FilteringRDDByQuery {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    // Data in the cache must be encoded with protobuf for the querying DSL
    // Declaring Protocol Buffer message types: schema
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
        new ConfigurationBuilder().addServer()
          .host("127.0.0.1").port(11222)
          .host("127.0.0.1").port(11372)
          .marshaller(new ProtoStreamMarshaller)
          .build()
      )
      // Registering a Protocol Buffers schema file
      rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME).put("test.proto", protoFile)

      val serCtx = ProtoStreamMarshaller.getSerializationContext(rcm)
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test.proto", protoFile))
      serCtx.registerMarshaller(new AddressMarshaller)
      serCtx.registerMarshaller(new PersonMarshaller)
      rcm
    }

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    //
    // Populate sample data into cache
    //
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    val defaultCache = cacheManager.getCache[Int, Person]
    // Remove all data
    defaultCache.clear()
    // Populate sample data
    (1 to 100).foreach { idx =>
      defaultCache.put(idx, new Person(s"name$idx", idx, new Address(s"street$idx", idx, "N/A")));
    }






    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    //
    // Start real example code from here
    //
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    val protoConfig = {
      val map = new util.HashMap[String, String]()
      map.put("test.proto", protoFile)
      map
    }

    val infinispanHost = "127.0.0.1:11222;127.0.0.1:11372"

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("jdg-spark-connector-example-filter-RDD-scala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val configuration = new Properties
    configuration.put("infinispan.rdd.query.proto.protofiles", protoConfig)
    configuration.put("infinispan.rdd.query.proto.marshallers", Seq(classOf[AddressMarshaller], classOf[PersonMarshaller]))
    configuration.put("infinispan.client.hotrod.server_list", infinispanHost)

    val infinispanRDD = new InfinispanRDD[Int, Person](sc, configuration)




    val query = Search.getQueryFactory(defaultCache)
      //      .from(classOf[Person]).having("address.number").gt(10)
      .from(classOf[Person]).having("name").like("name1%")
      .toBuilder[RemoteQuery].build()

    val filteredRDD = infinispanRDD.filterByQuery[Person](query, classOf[Person])

    //    filteredRDD.foreach(println)

    val filteredPersonRDD: RDD[Person] = filteredRDD.values

    val count = filteredPersonRDD.count()

    println()
    println("####################################")
    println(s"The filteredPersonRDD has $count records.")
    println("####################################")
    println()


    println()
    println("####################################")
    println("[Debug RDD]")

    filteredPersonRDD.foreach(x => {
      val name = x.getName
      println(name)
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
