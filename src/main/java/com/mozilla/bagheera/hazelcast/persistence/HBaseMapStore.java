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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.util.IdUtil;

/**
 * An implementation of Hazelcast's MapStore interface that persists map data to
 * HBase.
 * An arbitrary number of maps can be persisted into the same HBase table. To do
 * this, do not specify a column qualifer in the map configuration: the map name
 * will be used.
 */
public class HBaseMapStore extends MapStoreBase implements MapStore<String, String> {

    private static final Logger LOG = Logger.getLogger(HBaseMapStore.class);

    protected HTablePool pool;
    
    private byte[] tableName;
    private byte[] family;
    private byte[] qualifier;
    
    private boolean prefixDate;
    
    /* (non-Javadoc)
     * @see com.mozilla.bagheera.hazelcast.persistence.MapStoreBase#init(com.hazelcast.core.HazelcastInstance, java.util.Properties, java.lang.String)
     */
    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);

        Configuration conf = HBaseConfiguration.create();
        for (String name : properties.stringPropertyNames()) {
            LOG.info("property name: " + name + " value: " + properties.getProperty(name));
            if (name.startsWith("hbase.") || name.startsWith("hadoop.") || name.startsWith("zookeeper.")) {
                conf.set(name, properties.getProperty(name));
            }
        }

        prefixDate = Boolean.parseBoolean(properties.getProperty("hazelcast.hbase.key.prefix.date", "false"));
        int hbasePoolSize = Integer.parseInt(properties.getProperty("hazelcast.hbase.pool.size", "10"));
        tableName = Bytes.toBytes(properties.getProperty("hazelcast.hbase.table", mapName));
        family = Bytes.toBytes(properties.getProperty("hazelcast.hbase.column.family", "data"));
        qualifier = Bytes.toBytes(properties.getProperty("hazelcast.hbase.column.qualifier", "json"));
        
        pool = new HTablePool(conf, hbasePoolSize);
        
        try {
            HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
            if (!hbaseAdmin.tableExists(tableName)) {
                HTableDescriptor desc = new HTableDescriptor(tableName);
                HColumnDescriptor columnDesc = new HColumnDescriptor(family);
                columnDesc.setCompressionType(Algorithm.LZO);
                columnDesc.setBlockCacheEnabled(true);
                columnDesc.setBlocksize(65536);
                columnDesc.setInMemory(false);
                columnDesc.setMaxVersions(1);
                columnDesc.setTimeToLive(Integer.MAX_VALUE);
                desc.addFamily(columnDesc);
                hbaseAdmin.createTable(desc);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error creating table!", e);
        }
        
        // register with MapStoreRepository
        MapStoreRepository.addMapStore(mapName, this);
    }

    /* (non-Javadoc)
     * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
     */
    @Override
    public void destroy() {
        if (pool != null) {
            pool.closeTablePool(tableName);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.hazelcast.core.MapLoader#load(java.lang.Object)
     */
    @Override
    public String load(String key) {
        String retval = null;
        if (allowLoad) {
            HTableInterface table = null;
            try {
                Get g = new Get(Bytes.toBytes(key));
                table = pool.getTable(tableName);
                Result r = table.get(g);
                byte[] value = r.getValue(family, qualifier);
                if (value != null) {
                    if (outputFormatType == StoreFormatType.SMILE) {
                        retval = jsonSmileConverter.convertFromSmile(value);
                    } else {
                        retval = new String(value);
                    }
                }
            } catch (IOException e) {
                LOG.error("Value did not exist for row: " + key, e);
            } finally {
                if (pool != null && table != null) {
                    pool.putTable(table);
                }
            }
        }
        
        return retval;
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

            HTableInterface table = null;
            Map<String,String> kvMap = new HashMap<String,String>();
            try {
                table = pool.getTable(tableName);
                table.get(gets);
                Result[] result = table.get(gets);
                for (Result r : result) {
                    byte[] value = r.getValue(family, qualifier);
                    if (value != null) {
                        if (outputFormatType == StoreFormatType.SMILE) {
                            kvMap.put(new String(r.getRow()), jsonSmileConverter.convertFromSmile(r.getValue(family, qualifier)));
                        } else {
                            kvMap.put(new String(r.getRow()), new String(r.getValue(family, qualifier)));
                        }
                    }
                }
            } catch (IOException e) {
                LOG.error("IOException while getting values", e);
            } finally {
                if (pool != null && table != null) {
                    pool.putTable(table);
                }
            }
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
                hti = pool.getTable(tableName);
                Scan s = new Scan();
                s.addColumn(family, qualifier);
                ResultScanner rs = hti.getScanner(s);
                Result r = null;
                while ((r = rs.next()) != null) {
                    String k = new String(r.getRow());
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
            HTableInterface table = null;
            try {
                Delete d = new Delete(Bytes.toBytes(key));
                table = pool.getTable(tableName);
                table.delete(d);
            } catch (IOException e) {
                LOG.error("IOException while deleting key: " + key, e);
            } finally {
                if (table != null) {
                    pool.putTable(table);
                }
            }
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
            HTableInterface table = null;
            try {
                List<Delete> deletes = new ArrayList<Delete>();
                for (String k : keys) {
                    Delete d = new Delete(Bytes.toBytes(k));
                    deletes.add(d);
                }
                table = pool.getTable(tableName);
                table.delete(deletes);
            } catch (IOException e) {
                LOG.error("IOException while deleting values", e);
            } finally {
                if (table != null) {
                    pool.putTable(table);
                }
            }
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
        HTableInterface table = null;
        try {
            table = pool.getTable(tableName);
            try {
                byte[] rowId = prefixDate ? IdUtil.bucketizeId(key) : Bytes.toBytes(key);
                Put p = new Put(rowId);
                if (outputFormatType == StoreFormatType.SMILE) {
                    p.add(family, qualifier, jsonSmileConverter.convertToSmile(value));
                } else {
                    p.add(family, qualifier, Bytes.toBytes(value));
                }
                table.put(p);
            } catch (NumberFormatException nfe) {
                LOG.error("Encountered bad key: " + key, nfe);
            }
        } catch (IOException e) {
            LOG.error("Error during put", e);
        } finally {
            if (table != null) {
                pool.putTable(table);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.hazelcast.core.MapStore#storeAll(java.util.Map)
     */
    @Override
    public void storeAll(Map<String, String> pairs) {
        HTable table = null;
        try {
            List<Put> puts = new ArrayList<Put>(pairs.size());
            for (Map.Entry<String, String> pair : pairs.entrySet()) {
                try {
                    byte[] rowId = prefixDate ? IdUtil.bucketizeId(pair.getKey()) : Bytes.toBytes(pair.getKey());
                    Put p = new Put(rowId);
                    if (outputFormatType == StoreFormatType.SMILE) {
                        p.add(family, qualifier, jsonSmileConverter.convertToSmile(pair.getValue()));
                    } else {
                        p.add(family, qualifier, Bytes.toBytes(pair.getValue()));
                    }
                    puts.add(p);
                } catch (NumberFormatException nfe) {
                    LOG.error("Encountered bad key: " + pair.getKey(), nfe);
                }
            }
            
            table = (HTable) pool.getTable(tableName);
            table.setAutoFlush(false);
            table.put(puts);
            table.flushCommits();
        } catch (IOException e) {
            LOG.error("Error during puts", e);
        } finally {
            if (table != null) {
                pool.putTable(table);
            }
        }
    }

}
