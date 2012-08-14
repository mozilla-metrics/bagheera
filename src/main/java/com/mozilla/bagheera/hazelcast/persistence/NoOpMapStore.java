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
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;

/**
 * An implementation of Hazelcast's MapStore interface that only logs when
 * methods are called with parameter values. This is only used for debugging and
 * testing.
 */
public class NoOpMapStore extends MapStoreBase implements MapStore<String, String> {

    private static final Logger LOG = Logger.getLogger(NoOpMapStore.class);

    private Random rand;
    private double failureRate = 0.0;
    
    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);
        
        rand = new Random();
        failureRate = Double.parseDouble(properties.getProperty("hazelcast.store.failure.rate", "0.0"));
    }
    
    @Override
    public void destroy() {
    }
    
    @Override
    public String load(String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("load(%s)", key));
        }
        return null;
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("loadAll called with %d values", keys.size()));
        }
        for (String k : keys) {
            load(k);
        }
        return null;
    }

    @Override
    public Set<String> loadAllKeys() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("loadAllKeys called");
        }
        return null;
    }

    @Override
    public void delete(String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("delete(%s)", key));
        }
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("deleteAll called with %d values", keys.size()));
        }
        for (String k : keys) {
            delete(k);
        }
    }

    @Override
    public void store(String key, String value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("store(%s, %s)", key, value));
        }
        if (rand.nextDouble() < failureRate) {
            isHealthy = false;
            LOG.error("Simulated store failure");
        } else {
            isHealthy = true;
            LOG.info("Simulated store success");
        }
    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("storeAll called with %d values", pairs.size()));
        }
        for (Map.Entry<String, String> entry : pairs.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

}
