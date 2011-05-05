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

public class NoOpMapStore implements MapStore<String, String> {
    private static final Logger logger = Logger.getLogger(HBaseMapStore.class);

	@Override
	public String load(String key) {
		logger.info(String.format("load called\nkey: %s", key));
		return null;
	}

	@Override
	public Map<String, String> loadAll(Collection<String> keys) {
		logger.info(String.format("loadAll called with %d keys", keys.size()));
		return null;
	}

	@Override
	public void store(String key, String value) {
		logger.info(String.format("store called\nkey: %s\nvalue: %s", key, value));
	}

	@Override
	public void storeAll(Map<String, String> map) {
		logger.info(String.format("storeAll called with %d entries", map.size()));
	}

	@Override
	public void delete(String key) {
		logger.info(String.format("delete called\nkey: %s", key));
	}

	@Override
	public void deleteAll(Collection<String> keys) {
		logger.info(String.format("storeAll called with %d entries", keys.size()));
	}

}
