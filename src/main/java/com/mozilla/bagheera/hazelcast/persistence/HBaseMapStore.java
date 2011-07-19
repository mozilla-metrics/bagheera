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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.HBaseTableDao;

/**
 * An implementation of Hazelcast's MapStore interface that persists map data to
 * HBase. Due to the general size of data in HBase there is no interest for this
 * particular implementation to ever load keys. Therefore only the store and
 * storeAll methods are implemented.
 */
public class HBaseMapStore extends MapStoreBase implements MapStore<String, String>, MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(HBaseMapStore.class);

    protected HTablePool pool;
    protected HBaseTableDao table;

    /* (non-Javadoc)
     * @see com.mozilla.bagheera.hazelcast.persistence.MapStoreBase#init(com.hazelcast.core.HazelcastInstance, java.util.Properties, java.lang.String)
     */
    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);
        
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
    }

    /* (non-Javadoc)
     * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
     */
    @Override
    public void destroy() {
        if (pool != null) {
            pool.closeTablePool(table.getTableName());
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapLoader#load(java.lang.Object)
     */
    @Override
    public String load(String key) {
        return table.get(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapLoader#loadAll(java.util.Collection)
     */
    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        if (allowLoadAll) {
            List<Get> gets = new ArrayList<Get>(keys.size());
            for (String k : keys) {
                Get g = new Get(Bytes.toBytes(k));
                gets.add(g);
            }
            
            return table.getAll(keys);
        }
        
        return null;
    }

    /* (non-Javadoc)
     * @see com.hazelcast.core.MapLoader#loadAllKeys()
     */
    @Override
    public Set<String> loadAllKeys() {
        Set<String> keySet = null;
        if (allowLoadAll) {
            keySet = new HashSet<String>();
            HTableInterface hti = null;
            try {
                hti = pool.getTable(table.getTableName());
                Scan s = new Scan();
                s.addColumn(table.getColumnFamily(), table.getColumnQualifier());
                ResultScanner rs = hti.getScanner(s);
                Result r = null;
                Map<String,String> hzMap = Hazelcast.getMap(mapName);
                while ((r = rs.next()) != null) {
                    String k = new String(r.getRow());
                    hzMap.put(k, new String(r.getValue(table.getColumnFamily(), table.getColumnQualifier())));
                    keySet.add(k);
                }
            } catch (IOException e) {
                LOG.error("IOException while loading all keys", e);
            } finally {
                if (hti != null) {
                    pool.putTable(hti);
                }
            }
        }
        return keySet;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#delete(java.lang.Object)
     */
    @Override
    public void delete(String key) {
        if (allowDelete) {
            table.delete(key);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#deleteAll(java.util.Collection)
     */
    @Override
    public void deleteAll(Collection<String> keys) {
        if (allowDelete) {
            table.deleteAll(keys);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#store(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void store(String key, String value) {
        try {
            table.put(Bytes.toBytes(key), Bytes.toBytes(value));
        } catch (IOException e) {
            LOG.error("Error during put", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#storeAll(java.util.Map)
     */
    @Override
    public void storeAll(Map<String, String> pairs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Thread %s - storing %d items", Thread.currentThread().getId(), pairs.size()));
        }

        long current = System.currentTimeMillis();
        try {
            table.putStringMap(pairs);
        } catch (IOException e) {
            LOG.error("Error during put", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Thread %s stored %d items in %dms", Thread.currentThread().getId(), pairs.size(),
                      (System.currentTimeMillis() - current)));
        }
    }

}
