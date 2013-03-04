# Bagheera #

Version: 0.9-SNAPSHOT

#### REST service for Mozilla Metrics. This service currently uses Apache Kafka as its backing data store, then provides a few implementations of Kafka consumers to pull and persist to various data sinks. ####


### Version Compatability ###
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [Kafka](http://incubator.apache.org/kafka) 0.7.1+
* [Protocol Buffers](https://developers.google.com/protocol-buffers) 2.4.1+
* [Hadoop](http://hadoop.apache.org) 0.20.2+
* [HBase](http://hbase.apache.org) 0.90+

### Prerequisites ###
* Protocol Buffers
* Zookeeper (for Kafka)
* Kafka
* Hadoop (if using HDFS based consumer)
* HBase (if using HBase based consumer)

### Building ###
To make a jar you can do:  

`mvn package`

The jar file is then located under `target`.

### Running an instance ###
**Make sure your Kafka and Zookeeper servers are running first (see Kafka documentation)**

In order to run bagheera on another machine you will probably want to use the _dist_ assembly like so:

`mvn assembly:assembly`

The zip file now under the `target` directory should be deployed to `BAGHEERA_HOME` on the remote server.

To run Bagheera you can use `bin/bagheera` or copy the init.d script by the same name from `bin/init.d` to `/etc/init.d`. The init script assumes an installation of bagheera at `/usr/lib/bagheera`, but this can be modified by changing the `BAGHEERA_HOME` variable near the top of that script. Here is an example of using the regular bagheera script:

`bin/bagheera 8080`

### REST Request Format ###
#####URI _/submit/namespace/id_ | _/1.0/submit/namespace/id_#####
POST/PUT 

* The _namespace_ is required and is only accepted if it is in the configured white-list.
* The _id_ is optional although if you provide it currently it needs to be a valid UUID unless id validation is disabled on the _namespace_. 
* The payload content length must be less than the configured maximum.

DELETE

* The _namespace_ is required and is only accepted if it is in the configured white-list.
* The _id_ is required although if you provide it currently it needs to be a valid UUID unless id validation is disabled on the _namespace_.

Here's the list of HTTP response codes that Bagheera could send back:

* 201 Created - Returns the id submitted/generated. (default)
* 403 Forbidden - Violated access restrictions. Most likely because of the method used.
* 413 Request Too Large - Request payload was larger than the configured maximum.
* 400 Bad Request - Returned if the POST/PUT failed validation in some manner.
* 404 Not Found - Returned if the URI path doesn't exist or if the URI was not in the proper format.
* 500 Server Error - General server error. Someone with access should look at the logs for more details.

### Example Bagheera Configuration (conf/bagheera.properties) ###
    # valid namespaces (whitelist only, comma separated)
    valid.namespaces=mynamespace,othernamespace
    max.content.length=1048576

### Example Kafka Producer Configuration (conf/kafka.producer.properties) ###
    # comma delimited list of ZK servers
    zk.connect=127.0.0.1:2181
    # use bagheera message encoder
    serializer.class=com.mozilla.bagheera.serializer.BagheeraEncoder
    # asynchronous producer
    producer.type=async
    # compression.code (0=uncompressed,1=gzip,2=snappy)
    compression.codec=2
    # batch size (one of many knobs to turn in kafka depending on expected data size and request rate)
    batch.size=100

### Example Kafka Consumer Configuration (conf/kafka.consumer.properties) ###
    # kafka consumer properties
    zk.connect=127.0.0.1:2181
    fetch.size=1048576
    #serializer.class=com.mozilla.bagheera.serializer.BagheeraDecoder
    # bagheera specific kafka consumer properties
    consumer.threads=2

### Notes on consumers ###
We currently use the consumers implemented here, but it may also be of interest to look at systems such as [Storm](https://github.com/nathanmarz/storm) to process the messages. Storm contains a Kafka spout (consumer) and there are at least a couple of HBase bolts (processing/sink) already out there.

### License ###
All aspects of this software are distributed under Apache Software License 2.0. See LICENSE file for full license text.

### Contributors ###

* Xavier Stevens ([@xstevens](http://twitter.com/xstevens))
* Daniel Einspanjer ([@deinspanjer](http://twitter.com/deinspanjer))
* Anurag Phadke ([@anuragphadke](http://twitter.com/anuragphadke))
* Mark Reid ([@reid_write](http://twitter.com/reid_write))
