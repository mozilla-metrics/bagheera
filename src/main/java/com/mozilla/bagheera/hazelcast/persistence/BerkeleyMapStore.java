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
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.impl.ThreadContext;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * An implementation of Hazelcast's MapStore interface that persists
 * map data to BerkeleyDB. Currently we have no interest for this particular 
 * implementation to ever load keys. Therefore only the store and storeAll 
 * methods are implemented.
 */
public class BerkeleyMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport {

	private static final Logger LOG = Logger.getLogger(BerkeleyMapStore.class);
	private volatile Database db = null;

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.HazelcastInstance, java.util.Properties, java.lang.String)
	 */
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

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
	 */
	@Override
	public void destroy() {
		db.close();
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#store(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void store(String key, String value) {
		ThreadContext tc = ThreadContext.get();
		db.put(null, new DatabaseEntry(tc.toByteArray(key)), new DatabaseEntry(value.getBytes()));
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#storeAll(java.util.Map)
	 */
	@Override
	public void storeAll(Map<String, String> entries) {
		LOG.info(Thread.currentThread().getId() + ": Storing " + entries.size() + " entries ");
		long current = System.currentTimeMillis();
		ThreadContext tc = ThreadContext.get();
		for (Map.Entry<String, String> entry : entries.entrySet()) {
			OperationStatus os = db.put(null, new DatabaseEntry(tc.toByteArray(entry.getKey())),
					new DatabaseEntry(entry.getValue().getBytes()));
			if (os != OperationStatus.SUCCESS) {
				throw new RuntimeException("No Success");

			}
		}
		LOG.info(Thread.currentThread().getId() + ": Stored " + entries.size() + " entries in "
				+ (System.currentTimeMillis() - current) + " ms");
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
	 * @see com.hazelcast.core.MapLoader#load(java.lang.Object)
	 */
	@Override
	public String load(String key) {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoader#loadAll(java.util.Collection)
	 */
	@Override
	public Map<String, String> loadAll(Collection<String> keys) {
		return null;
	}

  @Override
  public Set<String> loadAllKeys() {
    // TODO Auto-generated method stub
    return null;
  }

}
