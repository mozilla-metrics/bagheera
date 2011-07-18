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
package com.mozilla.bagheera.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

public class ClientFactory {

    public static final String ES_CONFIG_PATH = "hazelcast.elasticsearch.config.path";
    public static final String ES_CLUSTER_NAME = "hazelcast.elasticsearch.cluster.name";
    public static final String ES_STORE_DATA = "hazelcast.elasticsearch.store.data";
    public static final String ES_TRANSPORT_AUTO_DISCOVER = "hazelcast.elasticsearch.transport.autodiscover";
    public static final String ES_SERVER_LIST = "hazelcast.elasticsearch.server.list";
    
    public static final String ES_CONFIG_PATH_DEFAULT = "conf/elasticsearch.yml";
    public static final String ES_STORE_DATA_DEFAULT = "false";
    public static final String ES_TRANSPORT_AUTO_DISCOVER_DEFAULT = "false";
    
    public static int DEFAULT_TRANSPORT_SOCKET = 9200;
    
    private static ClientFactory INSTANCE;
    private Map<String,Client> clientInstanceMap;
    
    private ClientFactory() {
        clientInstanceMap = new HashMap<String,Client>();
    }
    
    /**
     * @param props
     * @return
     */
    private static Client createNodeClient(Properties props) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        String configFile = props.getProperty(ES_CONFIG_PATH, ES_CONFIG_PATH_DEFAULT);
        boolean storeData = Boolean.parseBoolean(props.getProperty(ES_STORE_DATA, ES_STORE_DATA_DEFAULT));
        if (configFile != null) {
            settingsBuilder.loadFromClasspath(configFile);
        }
        
        Node node = nodeBuilder().loadConfigSettings(true).settings(settingsBuilder.build()).data(storeData).node().start();
        
        return node.client();
    }
    
    /**
     * @param props
     * @return
     */
    private static Client createTransportClient(Properties props) {
        TransportClient tc = null;
        boolean autoDiscover = Boolean.parseBoolean(props.getProperty(ES_TRANSPORT_AUTO_DISCOVER, ES_TRANSPORT_AUTO_DISCOVER_DEFAULT));
        if (autoDiscover) {
            tc = new org.elasticsearch.client.transport.TransportClient(ImmutableSettings.settingsBuilder().put("client.transport.sniff", true));
        } else {
            tc = new org.elasticsearch.client.transport.TransportClient();
            String serverList = props.getProperty(ES_SERVER_LIST);
            for (String server : serverList.split(",")) {
                tc.addTransportAddress(new InetSocketTransportAddress(server, DEFAULT_TRANSPORT_SOCKET));
            }
        }
        
        return tc;
    }
    
    /**
     * @return
     */
    public static ClientFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ClientFactory();
        }
        
        return INSTANCE;
    }
    
    /**
     * @return
     */
    public void close(String mapName) {
        if (clientInstanceMap != null && clientInstanceMap.containsKey(mapName)) {
            Client c = clientInstanceMap.get(mapName);
            if (c != null) {
                c.close();
                clientInstanceMap.remove(mapName);
            }
        }
    }
    
    /**
     * @param props
     * @return
     */
    public Client getNodeClient(Properties props) {
        return createNodeClient(props);
    }
    
    /**
     * @param props
     * @param singleton
     * @return
     */
    public Client getNodeClient(String mapName, Properties props, boolean singleton) {
        if (singleton) {
            if (!clientInstanceMap.containsKey(mapName)) {
                clientInstanceMap.put(mapName, createNodeClient(props));
            }
            
            return clientInstanceMap.get(mapName);
        } else {
            return createNodeClient(props);
        }
    }
    
    /**
     * @param props
     * @param singleton
     * @return
     */
    public Client getTransportClient(String mapName, Properties props, boolean singleton) {
        if (singleton) {
            if (!clientInstanceMap.containsKey(mapName)) {
                clientInstanceMap.put(mapName, createTransportClient(props));
            }
            
            return clientInstanceMap.get(mapName);
        } else {
            return createTransportClient(props);
        }
    }
    
}
