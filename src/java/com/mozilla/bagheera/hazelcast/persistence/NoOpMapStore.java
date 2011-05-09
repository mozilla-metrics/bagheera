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

import org.apache.log4j.Logger;

import com.hazelcast.core.MapStore;

/**
 * An implementation of Hazelcast's MapStore interface that only logs when
 * methods are called with parameter values. This is only used for debugging and
 * testing.
 */
public class NoOpMapStore implements MapStore<String, String> {
    
	private static final Logger LOG = Logger.getLogger(HBaseMapStore.class);

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoader#load(java.lang.Object)
	 */
	@Override
	public String load(String key) {
		LOG.info(String.format("load called\nkey: %s", key));
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapLoader#loadAll(java.util.Collection)
	 */
	@Override
	public Map<String, String> loadAll(Collection<String> keys) {
		LOG.info(String.format("loadAll called with %d keys", keys.size()));
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#store(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void store(String key, String value) {
		LOG.info(String.format("store called\nkey: %s\nvalue: %s", key, value));
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#storeAll(java.util.Map)
	 */
	@Override
	public void storeAll(Map<String, String> map) {
		LOG.info(String.format("storeAll called with %d entries", map.size()));
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#delete(java.lang.Object)
	 */
	@Override
	public void delete(String key) {
		LOG.info(String.format("delete called\nkey: %s", key));
	}

	/* (non-Javadoc)
	 * @see com.hazelcast.core.MapStore#deleteAll(java.util.Collection)
	 */
	@Override
	public void deleteAll(Collection<String> keys) {
		LOG.info(String.format("storeAll called with %d entries", keys.size()));
	}

}
