# Bagheera #

Version: 0.2  

#### REST service for Mozilla Metrics. This service currently uses Hazelcast as a distributed in-memory map with short TTLs. Then provides an implementation for Hazelcast MapStore to persist the map data to HBase. ####


### Version Compatability ###
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [Hadoop](http://hadoop.apache.org) 0.20.2+
* [HBase](http://hbase.apache.org) 0.90+
* [Hazelcast](http://www.hazelcast.com/) 1.9.2

### Building ###
To make a jar you can do:  

`ant jar`

The jar file is then located under build/lib.

### Running an instance ###
In order to run bagheera on another machine you need to deploy the following to your deployment target which I'll call _BAGHEERA_HOME_.

<table border="1">
	<tr>
		<th>Source</th>
		<th>Destination</th>
	</tr>
	<tr>
		<td>bin/run-jetty.sh</td>
		<td>$BAGHEERA_HOME/bin/run-jetty.sh</td>
	</tr>
	<tr>
		<td>lib/*</td>
		<td>$BAGHEERA_HOME/lib/</td>
	</tr>
	<tr>
		<td>build/lib/bagheera-*.jar</td>
		<td>$BAGHEERA_HOME/</td>
	</tr>
	<tr>
		<td>conf/hazelcast.xml</td>
		<td>$BAGHEERA_HOME/conf/</td>
	</tr>
</table> 

If you have the ability to scp to the remote host you can use the deploy/deploy-fresh build target to do these steps for you:

`ant -Dhostname=targethostname deploy-fresh`

The deploy target does everything that deploy-fresh does except copy the lib directory. This can save a lot of copy time if you don't have any library updates.
 
To run bagheera you can use bin/run-jetty.sh for now. We plan on adding some actual init scripts soon.

`bin/run-jetty.sh bagheera-0.1.jar 8080 conf/hazelcast.xml.example`

If you start up multiple instances Hazelcast will auto-discover other instances assuming your network and hazelcast.xml are setup to do so.

### Hazelcast HBaseMapStore Configuration ###

Suppose you've created a table called 'mytable' in HBase like so:

`create 'mytable', {NAME => 'data', COMPRESSION => 'LZO', VERSIONS => '1', TTL => '2147483647', BLOCKSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true'}`

And then you want to configure Bagheera and Hazelcast to write-behind the data it receives at _/submit/mytable/unique-id_. All you need to do is add a section like this to the hazelcast.xml configuration file:

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

To read more on Hazelcast configuration in general [check out their documentation](http://www.hazelcast.com/).

### License ###
All aspects of this software written in Java are distributed under Apache Software License 2.0. See LICENSE file for full license text.  
All aspects of this software written in Python are distributed under the [Mozilla Public License](http://www.mozilla.org/MPL/) MPL/LGPL/GPL tri-license.

### Contributors ###

* Xavier Stevens ([@xstevens](http://twitter.com/xstevens))
* Daniel Einspanjer ([@deinspanjer](http://twitter/deinspanjer))