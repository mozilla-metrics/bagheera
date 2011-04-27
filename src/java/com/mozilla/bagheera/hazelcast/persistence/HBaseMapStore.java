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

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

public class HBaseMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport {

    private static final Logger logger = Logger.getLogger(HBaseMapStore.class);
	private HTablePool pool;
	private byte[] table;
	private byte[] family;
	private byte[] qualifier;

    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		Configuration conf = HBaseConfiguration.create();
    	for (String name : properties.stringPropertyNames()) {
    		if (name.startsWith("hbase.") || name.startsWith("hadoop.")) {
    			conf.set(name, properties.getProperty(name));
    		}
		}
		int hbasePoolSize = Integer.parseInt(properties.getProperty("hazelcast.hbase.pool.size", "10"));
		table = Bytes.toBytes(properties.getProperty("hazelcast.hbase.table", "default"));
		family = Bytes.toBytes(properties.getProperty("hazelcast.hbase.column.family", "json"));
		qualifier = Bytes.toBytes(properties.getProperty("hazelcast.hbase.column.qualifier", ""));
		pool = new HTablePool(conf, hbasePoolSize);
    }

    public void destroy() {
    }

	@Override
	public String load(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> loadAll(Collection<String> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(String key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteAll(Collection<String> keys) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void store(String key, String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeAll(Map<String, String> pairs) {
		// TODO Auto-generated method stub
		
	}

}
