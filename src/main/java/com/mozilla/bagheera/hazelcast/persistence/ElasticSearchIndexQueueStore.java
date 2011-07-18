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
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;

public class ElasticSearchIndexQueueStore extends ComplexMapStoreBase implements MapStore<Long, String> {

    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexQueueStore.class);

    private String QUEUE_NAME;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);
        QUEUE_NAME = properties.getProperty("hazelcast.queue.name", "tasks");
    }

    @Override
    public String load(Long key) {
        return null;
    }

    @Override
    public Map<Long, String> loadAll(Collection<Long> key) {
        return null;
    }

    @Override
    public Set<Long> loadAllKeys() {
        return null;
    }

    @Override
    public void delete(Long key) {
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
    }

    @Override
    public void store(Long key, String value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("store(%s, %s)", key, value));
        }
        fetchAndIndex(value);
    }

    @Override
    public void storeAll(Map<Long, String> pairs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("storeAll called with %d values", pairs.size()));
        }
        
        fetchAndIndex(pairs.values());
        Queue<String> q = Hazelcast.getQueue(QUEUE_NAME);
        q.removeAll(pairs.values());
    }

}
