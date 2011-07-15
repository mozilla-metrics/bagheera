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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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

/**
 * An implementation of Hazelcast's MapStore interface that takes an ID from a
 * Hazelcast queue, gets the ID's value in HBase and then adds that value to an
 * ElasticSearch index. Currently we have no interest for this particular
 * implementation to ever load keys. Therefore only the store and storeAll
 * methods are implemented.
 */
public class ElasticSearchIndexMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport {
    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexMapStore.class);

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
        LOG.info("mapstore: received something in queue for store:" + key);

        List<String> rowIds = new ArrayList<String>();
        if (StringUtils.isNotBlank(key)) {
            rowIds.add(key);
            indexJsons(rowIds);
            rowIds.clear();

        }

    }

    @Override
    public void storeAll(Map<String, String> pairs) {
        LOG.info("mapstore: received something in queue for storeAll:" + pairs.size());
        List<String> rowIds = new ArrayList<String>();
        for (String row : pairs.keySet()) {
            if (StringUtils.isNotBlank(row)) {
                rowIds.add(row);
                if (rowIds.size() % 100 == 0) {
                    LOG.info("map size: " + pairs.size());
                    indexJsons(rowIds);
                    rowIds.clear();
                }
            }
        }
        if (rowIds.size() > 0) {
            LOG.info("map size: " + pairs.size());
            indexJsons(rowIds);
        }

    }

    private void indexJsons(List<String> rowIds) {
        LOG.debug("added rowIds: " + rowIds.size());
        Map<String, String> fetchedJsons = table.multipleGets(rowIds);

        LOG.info("Total # of docs getting indexed inside ElasticSearch: " + rowIds.size());
        if (fetchedJsons.size() > 0) {
            if (es.indexDocuments(fetchedJsons)) {
                LOG.debug("success indexing jsons inside ES, total count: " + fetchedJsons.size());
            }
        } else {
            LOG.debug("nothing to index");
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

    @Override
    public void destroy() {
        // TODO Auto-generated method stub
        if (pool != null) {
            pool.closeTablePool(table.getTableName());
        }

    }

}
