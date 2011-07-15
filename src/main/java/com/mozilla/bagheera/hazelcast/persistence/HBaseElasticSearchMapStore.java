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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.ElasticSearchDao;
import com.mozilla.bagheera.dao.HBaseTableDao;
import com.mozilla.bagheera.elasticsearch.NodeClientSingleton;

public class HBaseElasticSearchMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport {
    
    private static final Logger LOG = Logger.getLogger(HBaseElasticSearchMapStore.class);

    private HTablePool pool;
    private HBaseTableDao table;
    private ElasticSearchDao es;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        Configuration conf = HBaseConfiguration.create();
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("hbase.") || name.startsWith("hadoop.") || name.startsWith("zookeeper.")) {
                conf.set(name, properties.getProperty(name));
            }
        }

        int hbasePoolSize = Integer.parseInt(properties.getProperty("hazelcast.hbase.pool.size", "10"));
        String tableName = properties.getProperty("hazelcast.hbase.table", "default");
        String family = properties.getProperty("hazelcast.hbase.column.family", "data");
        String columnQualifier = properties.getProperty("hazelcast.hbase.column.qualifier");
        String qualifier = columnQualifier == null ? "" : columnQualifier;

        pool = new HTablePool(conf, hbasePoolSize);
        table = new HBaseTableDao(pool, tableName, family, qualifier);

        String indexName = properties.getProperty("hazelcast.elasticsearch.index", "default");
        String typeName = properties.getProperty("hazelcast.elasticsearch.type.name", "data");
        es = new ElasticSearchDao(NodeClientSingleton.getInstance(properties).getClient(), indexName, typeName);
    }

    public void destroy() {
        if (pool != null) {
            pool.closeTablePool(table.getTableName());
        }
        try {
            NodeClientSingleton.getInstance().close();
        } catch (IOException e) {
            LOG.error("Error closing ElasticSearch client", e);
        }
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
        try {
            table.put(key, value);
            es.indexDocument(key, value);
        } catch (IOException e) {
            LOG.error("Error during put", e);
        }
    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Thread %s - storing %d items", Thread.currentThread().getId(), pairs.size()));
        }

        long current = System.currentTimeMillis();
        try {
            table.putStringMap(pairs);
            es.indexDocuments(pairs);
        } catch (IOException e) {
            LOG.error("Error during put", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Thread %s stored %d items in %dms", Thread.currentThread().getId(), pairs.size(),
                    (System.currentTimeMillis() - current)));
        }
    }

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

}
