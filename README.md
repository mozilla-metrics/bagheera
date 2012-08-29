# Bagheera #

Version: 0.7-SNAPSHOT  

#### REST service for Mozilla Metrics. This service currently uses Apache Kafka as its backing data store, then provides a few implementations of Kafka consumers to pull and persist to various data sinks. ####


### Version Compatability ###
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [Hadoop](http://hadoop.apache.org) 0.20.2+
* [HBase](http://hbase.apache.org) 0.90+
* [Kafka](http://incubator.apache.org/kafka) 0.7.1+

### Building ###
To make a jar you can do:  

`mvn package`

The jar file is then located under `target`.

### Running an instance ###
TODO: Make sure your Kafka and Zookeeper servers are running first (see Kafka documentation)

In order to run bagheera on another machine you will probably want to use the _dist_ assembly like so: need to deploy the following to your deployment target which I'll call `BAGHEERA_HOME`.

`mvn assembly:assembly`

The zip file now under the `target` directory should be deployed to `BAGHEERA_HOME` on the remote server.

To run Bagheera you can use `bin/bagheera` or copy the init.d script by the same name from `bin/init.d` to `/etc/init.d`. The init script assumes an installation of bagheera at `/usr/lib/bagheera`, but this can be modified by changing the `BAGHEERA_HOME` variable near the top of that script. Here is an example of using the regular bagheera script:

`bin/bagheera 8080`

### REST Request Format ###

Bagheera takes POST data on _/namespace/uniqueid_. The _uniqueid_ is optional although if you provide it currently it needs to be a valid UUID. The POST body is currently required to be a valid JSON object although this requirement may change in the future.

Here's a quick rundown of HTTP return codes that Bagheera could send back (this isn't comprehensive but rather the most common ones):

* 201 Created - returned if everything was submitted successfully (default)
* 406 Not Acceptable - returned if the POST failed validation in some manner
* 500 Server Error - something went horribly wrong and you should check the logs

### Example Bagheera Configuration (conf/bagheera.properties) ###
    # valid namespaces (whitelist only)
    valid.namespaces=mynamespace
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
		
### License ###
All aspects of this software are distributed under Apache Software License 2.0. See LICENSE file for full license text.

### Contributors ###

* Xavier Stevens ([@xstevens](http://twitter.com/xstevens))
* Daniel Einspanjer ([@deinspanjer](http://twitter.com/deinspanjer))
* Anurag Phadke ([@anuragphadke](http://twitter.com/anuragphadke))
* Mark Reid ([@reid_write](http://twitter.com/reid_write))
