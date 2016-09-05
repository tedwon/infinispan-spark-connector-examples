# infinispan-spark-connector-examples

## Prerequisite: 

### 1. Install JDK 8
### 2. Download and Run Infinispan Server

* Download the latest version of Infinispan Server
  * http://infinispan.org/download/#stable
```sh
$ wget http://downloads.jboss.org/infinispan/8.2.4.Final/infinispan-server-8.2.4.Final-bin.zip
```
* Unzip and cd <ISPN_HOME>
* Run bin/add-user.sh
* Run bin/domain.sh

## Build

```sh
$ mvn clean package
```

## Run

* Run objects in the org.jbugkorea.spark.jdg.examples package like below:

```sh
$ mvn exec:java -Dexec.mainClass="org.jbugkorea.spark.jdg.examples.one.CreateRDDFromJDGCache"
```

## Spark application UI

* http://localhost:4040

## Infinispan Admin console 

* Go to Admin console http://localhost:9990

## References

* https://github.com/infinispan/infinispan-spark
* https://access.redhat.com/documentation/en-US/Red_Hat_JBoss_Data_Grid/7.0/html-single/Developer_Guide/index.html#Integration_with_Apache_Spark
* http://infinispan.org/tutorials/simple/spark/
* http://blog.infinispan.org/2015/08/infinispan-spark-connector-01-released.html
* http://infinispan.org/docs/stable/user_guide/user_guide.html
* http://spark.apache.org/docs/1.6.2/programming-guide.html
* https://hub.docker.com/r/gustavonalle/infinispan-spark/