/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mozilla.bagheera.hazelcast.persistence;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.nio.DataSerializable;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BerkeleyMapStore implements MapStore<String, DataSerializable>, MapLoaderLifecycleSupport {

	private static final Logger logger = Logger.getLogger(BerkeleyMapStore.class);
	volatile Database db = null;

	@Override
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(true);
		envConfig.setAllowCreate(true);
		Environment exampleEnv = new Environment(new File(properties.getProperty("bdb.home")), envConfig);
		Transaction txn = exampleEnv.beginTransaction(null, null);
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(true);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(true);
		db = exampleEnv.openDatabase(txn, "/" + mapName, dbConfig);
		txn.commit();
	}

	@Override
	public void destroy() {
		db.close();
	}

	@Override
	public void store(String key, DataSerializable value) {
		ThreadContext tc = ThreadContext.get();
		db.put(null, new DatabaseEntry(tc.toByteArray(key)), new DatabaseEntry(tc.toByteArray(value)));
	}

	@Override
	public void storeAll(Map<String, DataSerializable> entries) {
		logger.info(Thread.currentThread().getId() + ": Storing " + entries.size() + " entries ");
		long current = System.currentTimeMillis();
		ThreadContext tc = ThreadContext.get();
		for (Map.Entry<String, DataSerializable> entry : entries.entrySet()) {
			OperationStatus os = db.put(null, new DatabaseEntry(tc.toByteArray(entry.getKey())),
					new DatabaseEntry(tc.toByteArray(entry.getValue())));
			if (os != OperationStatus.SUCCESS) {
				throw new RuntimeException("No Success");

			}
		}
		logger.info(Thread.currentThread().getId() + ": Stored " + entries.size() + " entries in "
				+ (System.currentTimeMillis() - current) + " ms");
	}

	@Override
	public void delete(String key) {
	}

	@Override
	public void deleteAll(Collection<String> keys) {
	}

	@Override
	public DataSerializable load(String key) {
		return null;
	}

	@Override
	public Map<String, DataSerializable> loadAll(Collection<String> keys) {
		return null;
	}

}
