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

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.ElasticSearchDao;

public class ElasticSearchMapStore extends MapStoreBase implements MapStore<String,String> {

    public static final String ES_CONFIG_PATH = "hazelcast.elasticsearch.config.path";
    public static final String ES_CLUSTER_NAME = "hazelcast.elasticsearch.cluster.name";
    public static final String ES_LOCAL = "hazelcast.elasticsearch.local";
    public static final String ES_STORE_DATA = "hazelcast.elasticsearch.store.data";
    public static final String ES_TRANSPORT_AUTO_DISCOVER = "hazelcast.elasticsearch.transport.autodiscover";
    public static final String ES_SERVER_LIST = "hazelcast.elasticsearch.server.list";

    public static final String ES_LOCAL_DEFAULT = "false";
    public static final String ES_STORE_DATA_DEFAULT = "false";

    protected Node esNode;
    protected Client esClient;
    protected ElasticSearchDao es;

    private void initNodeClient(Properties properties) {
        NodeBuilder nodeBuilder = nodeBuilder();
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        String configFile = properties.getProperty(ES_CONFIG_PATH);
        if (configFile != null) {
            settingsBuilder.loadFromClasspath(configFile);
        } else {
            String clusterName = properties.getProperty(ES_CLUSTER_NAME);
            if (clusterName != null) {
                nodeBuilder.clusterName(clusterName);
            }

            boolean localMode = Boolean.parseBoolean(properties.getProperty(ES_LOCAL, ES_LOCAL_DEFAULT));
            nodeBuilder.local(localMode);

            boolean storeData = Boolean.parseBoolean(properties.getProperty(ES_STORE_DATA, ES_STORE_DATA_DEFAULT));
            nodeBuilder.client(!storeData).data(storeData);
        }

        esNode = nodeBuilder.loadConfigSettings(false).settings(settingsBuilder).node();
        esClient = esNode.client();
    }

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);

        String indexName = properties.getProperty("hazelcast.elasticsearch.index", mapName);
        String typeName = properties.getProperty("hazelcast.elasticsearch.type.name", "data");

        initNodeClient(properties);
        es = new ElasticSearchDao(esClient, indexName, typeName);
    }

    @Override
    public void destroy() {
        if (esClient != null) {
            esClient.close();
        }
        if (esNode != null) {
            esNode.close();
        }
    }

    @Override
    public String load(String key) {
        return es.get(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        return es.fetchAll(keys);
    }

    @Override
    public Set<String> loadAllKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(String key) {
        es.delete(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        es.delete(keys);
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
