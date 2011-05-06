# Bagheera v0.1 #
REST service for Mozilla Metrics. This service currently uses Hazelcast as a distributed in-memory map with short TTLs. Then provides an implementation for Hazelcast MapStore to persist the map data to HBase.


## Requirements ##
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

[Hadoop](http://hadoop.apache.org) 0.20.2+
[HBase](http://hbase.apache.org) 0.90+

## Building ##
To make a jar you can do:  

`ant jar`


## License ##
All aspects of this software written in Java are distributed under Apache Software License 2.0. See the LICENSE file for details.
All aspects of this software written in Python are distributed under the [Mozilla Public License](http://www.mozilla.org/MPL/) MPL/LGPL/GPL tri-license.