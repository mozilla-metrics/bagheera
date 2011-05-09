/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.hazelcast.persistence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.impl.ascii.rest.RestValue;
import com.mozilla.bagheera.dao.HBaseTableDao;

/**
 * An implementation of Hazelcast's MapStore interface that persists
 * map data to HBase. Due to the general size of data in HBase there is 
 * no interest for this particular implementation to ever load keys. Therefore
 * only the store and storeAll methods are implemented.
 */
public class HBaseMapStore implements MapStore<String, RestValue>, MapLoaderLifecycleSupport {

	private static final Logger LOG = Logger.getLogger(HBaseMapStore.class);
	
	private HTablePool pool;
	private HBaseTableDao table;

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.HazelcastInstance, java.util.Properties, java.lang.String)
	 */
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		Configuration conf = HBaseConfiguration.create();
		for (String name : properties.stringPropertyNames()) {
			if (name.startsWith("hbase.") || name.startsWith("hadoop.") || name.startsWith("zookeeper.")) {
				conf.set(name, properties.getProperty(name));
			}
		}
		
		int hbasePoolSize = Integer.parseInt(properties.getProperty("hazelcast.hbase.pool.size", "10"));
		String tableName = properties.getProperty("hazelcast.hbase.table", "default");
		String family = properties.getProperty("hazelcast.hbase.column.family", "json");
		String columnQualifier = properties.getProperty("hazelcast.hbase.column.qualifier");
		String qualifier = columnQualifier == null ? "" : columnQualifier;
		
		pool = new HTablePool(conf, hbasePoolSize);
		table = new HBaseTableDao(pool, tableName, family, qualifier);
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
	 */
	public void destroy() {
		pool.closeTablePool(table.getTableName());
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoader#load(java.lang.Object)
	 */
	@Override
	public RestValue load(String key) {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoader#loadAll(java.util.Collection)
	 */
	@Override
	public Map<String, RestValue> loadAll(Collection<String> keys) {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#delete(java.lang.Object)
	 */
	@Override
	public void delete(String key) {
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#deleteAll(java.util.Collection)
	 */
	@Override
	public void deleteAll(Collection<String> keys) {
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#store(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void store(String key, RestValue value) {
		try {
			table.put(Bytes.toBytes(key), value.getValue());
		} catch (IOException e) {
			LOG.error("Error during put", e);
		}
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#storeAll(java.util.Map)
	 */
	@Override
	public void storeAll(Map<String, RestValue> pairs) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Thread %s - storing %d items", Thread.currentThread().getId(), pairs.size()));
		}
		
		long current = System.currentTimeMillis();
		List<Put> puts = new ArrayList<Put>();
		for (Map.Entry<String, RestValue> pair : pairs.entrySet()) {
			//byteMap.put(Bytes.toBytes(p.getKey()), p.getValue().getValue());
			Put p = new Put(Bytes.toBytes(pair.getKey()));
			p.add(table.getColumnFamily(), table.getColumnQualifier(), pair.getValue().getValue());
			puts.add(p);
		}

		try {
			table.putList(puts);
		} catch (IOException e) {
			LOG.error("Error during put", e);
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.info(String.format("Thread %s stored %d items in %dms", Thread.currentThread().getId(), pairs.size(),
				(System.currentTimeMillis() - current)));
		}
	}

}
