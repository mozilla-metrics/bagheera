# Bagheera #

Version: 0.4  

#### REST service for Mozilla Metrics. This service currently uses Hazelcast as a distributed in-memory map with short TTLs. Then provides an implementation for Hazelcast MapStore to persist the map data to various data sinks. ####


### Version Compatability ###
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [Hadoop](http://hadoop.apache.org) 0.20.2+
* [HBase](http://hbase.apache.org) 0.90+
* [Hazelcast](http://www.hazelcast.com/) 1.9.3
* [Elastic Search](http://www.elasticsearch.org/) 0.17.6

### Building ###
To make a jar you can do:  

`mvn package`

The jar file is then located under `target`.

### Running an instance ###
In order to run bagheera on another machine you will probably want to use the _dist_ assembly like so: need to deploy the following to your deployment target which I'll call `BAGHEERA_HOME`.

`mvn assembly:assembly`

The zip file now under the `target` directory should be deployed to `BAGHEERA_HOME` on the remote server.

To run Bagheera you can use `bin/bagheera` or copy the init.d script by the same name from `bin/init.d` to `/etc/init.d`. The init script assumes an installation of bagheera at `/usr/lib/bagheera`, but this can be modified by changing the `BAGHEERA_HOME` variable near the top of that script. Here is an example of using the regular bagheera script:

`bin/bagheera 8080 conf/hazelcast.xml.example`

If you start up multiple instances Hazelcast will auto-discover other instances assuming your network and hazelcast.xml are setup to do so.

### REST Request Format ###

Bagheera takes POST data on _/map/mymapname/unique-id_. Depending on how _mymapname_ is configured in the Hazelcast configuration file, it may write to different sources. That is explained further below.

Here's a quick rundown of HTTP return codes that Bagheera could send back (this isn't comprehensive but rather the most common ones):

* 201 Created - returned if everything was submitted successfully and the mapname.post.reponse property in bagheera.properties is set to true
* 204 No Content - returned if everything was submitted successfully and the mapname.post.response property in bagheera.properties is set to false (default)
* 406 Not Acceptable - returned if the POST failed validation in some manner

### Hazelcast HBaseMapStore Configuration ###

Suppose you've created a table called 'mytable' in HBase like so:

`create 'mytable', {NAME => 'data', COMPRESSION => 'LZO', VERSIONS => '1', TTL => '2147483647', BLOCKSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true'}`

All you need to do is add a section like this to the hazelcast.xml configuration file:

	<map name="mytable">
		<time-to-live-seconds>20</time-to-live-seconds>
		<backup-count>1</backup-count>
		<eviction-policy>NONE</eviction-policy>
		<max-size>0</max-size>
		<eviction-percentage>25</eviction-percentage>
		<merge-policy>hz.ADD_NEW_ENTRY</merge-policy>
		<!-- HBaseMapStore -->
		<map-store enabled="true">
			<class-name>com.mozilla.bagheera.hazelcast.persistence.HBaseMapStore</class-name>
			<write-delay-seconds>5</write-delay-seconds>
			<property name="hazelcast.hbase.pool.size">20</property>
			<property name="hazelcast.hbase.table">mytable</property>
			<property name="hazelcast.hbase.column.family">data</property>
			<property name="hazelcast.hbase.column.qualifier">json</property>
		</map-store>
	</map>

Notice you can tweak the HBase connection pool size, table and column names as needed for different maps.

### Hazelcast HdfsMapStore Configuration ###

If you want to configure a Hazelcast Map to persist data to HDFS you can use the HdfsMapStore. It will write a SequenceFile with Text key/value pairs. Currently it will always use block compression. In the future we may add support for more compression codecs or alternative file formats. This MapStore will rollover and write new files every day or when _hazelcast.hdfs.max.filesize_ is reached. It will write files to the directory _hazelcast.hdfs.basedir/hazelcast.hdfs.dateformat/UUID_. Please note that _hazelcast.hdfs.max.filesize_ is only checked against a bytes written counter and not the actual filesize in HDFS. Actual filesize's will probably be much smaller than this number due to block compression. Here is an example section using this MapStore from hazelcast.xml configuration:

	<map name="mymapname">
		<time-to-live-seconds>20</time-to-live-seconds>
		<backup-count>1</backup-count>
		<eviction-policy>NONE</eviction-policy>
		<max-size>0</max-size>
		<eviction-percentage>25</eviction-percentage>
		<merge-policy>hz.ADD_NEW_ENTRY</merge-policy>
		<!-- HdfsMapStore -->
		<map-store enabled="true">
			<class-name>com.mozilla.bagheera.hazelcast.persistence.HdfsMapStore</class-name>
			<write-delay-seconds>5</write-delay-seconds>
			<property name="hazelcast.hdfs.basedir">/bagheera</property>
			<property name="hazelcast.hdfs.dateformat">yyyy-MM-dd</property>
			<property name="hazelcast.hdfs.max.filesize">1073741824</property>
		</map-store>
	</map>

### Hazelcast ElasticSearchMapStore Configuration ###
If you want to configure a Hazelcast Map to persist data to ElasticSearch you can use the ElasticSearchMapStore. It will start a Node using the specified ElasticSearch config and index the data submitted to this map. This is probably of limited use if you're only using ElasticSearch as you could just use their interface directly.  Here's an example configuration:

	<map name="mymapname">
		<time-to-live-seconds>20</time-to-live-seconds>
		<backup-count>1</backup-count>
		<eviction-policy>NONE</eviction-policy>
		<max-size>0</max-size>
		<eviction-percentage>25</eviction-percentage>
		<merge-policy>hz.ADD_NEW_ENTRY</merge-policy>
		<!-- ElasticSearchMapStore -->
		<map-store enabled="true">
			<class-name>com.mozilla.bagheera.hazelcast.persistence.ElasticSearchMapStore</class-name>
			<write-delay-seconds>5</write-delay-seconds>
			<property name="hazelcast.elasticsearch.config.path">elasticsearch-socorro.yml</property>
			<property name="hazelcast.elasticsearch.index">socorro</property>
			<property name="hazelcast.elasticsearch.type.name">crash_reports</property>
		</map-store>
	</map>

### Hazelcast CompositeMapStore Configuration ###

The idea behind this store is that if you have data being inserted into HBase already you could post a row ID via REST to a map. Once the ID is received and the MapStore persistence is triggered we then want to take a column value from a HBase column and send that value to ElasticSearch for indexing. Here is an example section using this MapStore from hazelcast.xml configuration:

	<map name="mymapname">
		<time-to-live-seconds>20</time-to-live-seconds>
		<backup-count>1</backup-count>
		<eviction-policy>NONE</eviction-policy>
		<max-size>0</max-size>
		<eviction-percentage>25</eviction-percentage>
		<merge-policy>hz.ADD_NEW_ENTRY</merge-policy>
		<!-- CompositeMapStore -->
		<map-store enabled="true">
			<class-name>com.mozilla.bagheera.hazelcast.persistence.CompositeMapStore</class-name>
			<write-delay-seconds>5</write-delay-seconds>
			<property name="hazelcast.composite.load.class.name">com.mozilla.bagheera.hazelcast.persistence.HBaseMapStore</property>
			<property name="hazelcast.composite.store.class.name">com.mozilla.bagheera.hazelcast.persistence.ElasticSearchMapStore</property>
			<property name="hazelcast.elasticsearch.config.path">elasticsearch-socorro.yml</property>
			<property name="hazelcast.elasticsearch.index">socorro</property>
			<property name="hazelcast.elasticsearch.type.name">crash_reports</property>
			<property name="hazelcast.hbase.pool.size">20</property>
			<property name="hazelcast.hbase.table">crash_reports</property>
			<property name="hazelcast.hbase.column.family">processed_data</property>
			<property name="hazelcast.hbase.column.qualifier">json</property>
		</map-store>
	</map>

Unfortunately for now you cannot use the same type of MapStore as both the load MapStore and store MapStore (i.e. two HBaseMapStore(s)), because the configuration parameters don't allow for it. We hope to find a somewhat clean solution for this in the future.

### Hazelcast MultipleMapStore Configuration ###
This store is for when you have data you want to store in multiple data stores. Unlike CompositeMapStore this doesn't retrieve anything it only stores to all configured MapStore(s). Here is an example configuration using ElasticSearch and HBase:

	<map name="mymapname">
		<time-to-live-seconds>20</time-to-live-seconds>
		<backup-count>1</backup-count>
		<eviction-policy>NONE</eviction-policy>
		<max-size>0</max-size>
		<eviction-percentage>25</eviction-percentage>
		<merge-policy>hz.ADD_NEW_ENTRY</merge-policy>
		<!-- MultipleMapStore -->
		<map-store enabled="true">
			<class-name>com.mozilla.bagheera.hazelcast.persistence.MultiMapStore</class-name>
			<write-delay-seconds>5</write-delay-seconds>
			<property name="hazelcast.multi.store.class.name.1">com.mozilla.bagheera.hazelcast.persistence.HBaseMapStore</property>
			<property name="hazelcast.multi.store.class.name.2">com.mozilla.bagheera.hazelcast.persistence.ElasticSearchMapStore</property>
			<property name="hazelcast.elasticsearch.config.path">elasticsearch-socorro.yml</property>
			<property name="hazelcast.elasticsearch.index">socorro</property>
			<property name="hazelcast.elasticsearch.type.name">crash_reports</property>
			<property name="hazelcast.hbase.pool.size">20</property>
			<property name="hazelcast.hbase.table">crash_reports</property>
			<property name="hazelcast.hbase.column.family">processed_data</property>
			<property name="hazelcast.hbase.column.qualifier">json</property>
		</map-store>
	</map>

Unfortunately for now you cannot use the same type of MapStore more than once, because the configuration parameters don't allow for it. We hope to find a somewhat clean solution for this in the future.

To read more on Hazelcast configuration in general [check out their documentation](http://www.hazelcast.com/).

### License ###
All aspects of this software written in Java are distributed under Apache Software License 2.0. See LICENSE file for full license text.  
All aspects of this software written in Python are distributed under the [Mozilla Public License](http://www.mozilla.org/MPL/) MPL/LGPL/GPL tri-license.

### Contributors ###

* Xavier Stevens ([@xstevens](http://twitter.com/xstevens))
* Daniel Einspanjer ([@deinspanjer](http://twitter/deinspanjer))
* Anurag Phadke ([@anuragphadke](http://twitter.com/anuragphadke))