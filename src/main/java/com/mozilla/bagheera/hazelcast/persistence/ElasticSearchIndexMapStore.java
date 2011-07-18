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
import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

/**
 * An implementation of Hazelcast's MapStore interface that takes an ID from a
 * Hazelcast queue, gets the ID's value in HBase and then adds that value to an
 * ElasticSearch index. Currently we have no interest for this particular
 * implementation to ever load keys. Therefore only the store and storeAll
 * methods are implemented.
 */
public class ElasticSearchIndexMapStore extends ComplexMapStoreBase implements MapStore<String, String>, MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexMapStore.class);

    @Override
    public String load(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, String> loadAll(Collection<String> arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> loadAllKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(String arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteAll(Collection<String> arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void store(String key, String value) {
        fetchAndIndex(key);
    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        fetchAndIndex(pairs.keySet());
    }

}
