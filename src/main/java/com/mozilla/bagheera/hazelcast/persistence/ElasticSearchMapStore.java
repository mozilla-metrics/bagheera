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
import java.util.Set;

import org.elasticsearch.client.Client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.ElasticSearchDao;
import com.mozilla.bagheera.elasticsearch.ClientFactory;

public class ElasticSearchMapStore extends MapStoreBase implements MapStore<String,String>, MapLoaderLifecycleSupport {

    protected Client esClient;
    protected ElasticSearchDao es;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);
        
        String indexName = properties.getProperty("hazelcast.elasticsearch.index", "default");
        String typeName = properties.getProperty("hazelcast.elasticsearch.type.name", "data");
        esClient = ClientFactory.getInstance().getNodeClient(mapName, properties, true);
        es = new ElasticSearchDao(esClient, indexName, typeName);
    }
    
    @Override
    public void destroy() {
        ClientFactory.getInstance().close(mapName);
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
    public Set<String> loadAllKeys() {
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
        es.indexDocument(key, value);
    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        es.indexDocuments(pairs);
    }

}
