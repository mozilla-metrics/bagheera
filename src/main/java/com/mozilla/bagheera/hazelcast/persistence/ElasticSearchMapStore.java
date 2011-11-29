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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.ElasticSearchDao;

public class ElasticSearchMapStore extends MapStoreBase implements MapStore<String,String> {

    private static final Logger LOG = Logger.getLogger(ElasticSearchMapStore.class);

    private static final ClientProvider clients = new ClientProvider();

    public static final String ES_CONFIG_PATH = "hazelcast.elasticsearch.config.path";
    public static final String ES_CLUSTER_NAME = "hazelcast.elasticsearch.cluster.name";
    public static final String ES_LOCAL = "hazelcast.elasticsearch.local";
    public static final String ES_STORE_DATA = "hazelcast.elasticsearch.store.data";
    public static final String ES_TRANSPORT_AUTO_DISCOVER = "hazelcast.elasticsearch.transport.autodiscover";
    public static final String ES_SERVER_LIST = "hazelcast.elasticsearch.server.list";

    public static final String ES_LOCAL_DEFAULT = "false";
    public static final String ES_STORE_DATA_DEFAULT = "false";
    public static final String ES_LOAD_ALL_DEFAULT = "false";

    protected String nodeKey;
    protected Client esClient;
    protected ElasticSearchDao es;

    /**
     * Manages ElasticSearch node clients.
     *
     * Keeps around a single NodeClient for each configuration file and/or
     * cluster name given by clients. Discards node clients that are not
     * used anymore.
     */
    private static class ClientProvider {

        private final Map<String, Node> nodes = new HashMap<String, Node>();
        private final Map<String, Integer> numClients = new HashMap<String, Integer>();

        synchronized Client retain(String nodeKey, Properties properties) {
            if (nodes.containsKey(nodeKey)) {
                LOG.debug(String.format("Added referent to node key '%s'", nodeKey));
                numClients.put(nodeKey, numClients.get(nodeKey) + 1);
                return nodes.get(nodeKey).client();
            }

            NodeBuilder nodeBuilder = nodeBuilder();
            String configPath = properties.getProperty(ES_CONFIG_PATH);
            if (configPath != null) {
                ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
                nodeBuilder.settings(settingsBuilder.loadFromClasspath(configPath));
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

            Node node = nodeBuilder.loadConfigSettings(false).node();
            nodes.put(nodeKey, node);
            numClients.put(nodeKey, 1);
            LOG.debug(String.format("Added FIRST referent to node key '%s'", nodeKey));
            return node.client();
        }

        synchronized void release(String nodeKey, Client client) {
            if (!nodes.containsKey(nodeKey)) {
                LOG.warn(String.format("Cannot release nonexistent node key '%s'", nodeKey));
                return;
            }
            int n = numClients.get(nodeKey);
            if (n == 1) {
                Node node = nodes.get(nodeKey);
                client.close();
                node.close();
                numClients.remove(nodeKey);
            }
            numClients.put(nodeKey, n - 1);
            LOG.debug(String.format("Released referent to node key '%s'", nodeKey));
        }
    }

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);

        String indexName = properties.getProperty("hazelcast.elasticsearch.index", mapName);
        String typeName = properties.getProperty("hazelcast.elasticsearch.type.name", "data");

        if (indexName.endsWith("*")) {
            throw new IllegalArgumentException("Illegal index name: " + indexName);
        }

        nodeKey = properties.getProperty(ES_CONFIG_PATH);
        if (nodeKey == null) {
            nodeKey = properties.getProperty(ES_CLUSTER_NAME);
        }
        if (nodeKey == null) {
            LOG.error("Could not initialize ES map store: need config path or cluster name!");
            return;
        }
        esClient = clients.retain(nodeKey, properties);
        es = new ElasticSearchDao(esClient, indexName, typeName);
    }

    @Override
    public void destroy() {
        if (nodeKey == null) {
            LOG.warn("Tried to destroy uninitialized ES MapStore!");
            return;
        }
        clients.release(nodeKey, esClient);
        nodeKey = null;
        esClient = null;
        es = null;
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
        Set<String> keySet = null;
        if (allowLoadAll) {
            keySet = es.fetchAllKeys();
        }
        
        return keySet;
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
