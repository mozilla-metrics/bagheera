/*
 * Copyright 2012 Mozilla Foundation
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
package com.mozilla.bagheera.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.util.IdUtil;

public class HBaseSink implements KeyValueSink {
    
    private static final Logger LOG = Logger.getLogger(HBaseSink.class);

    protected long sleepTime = 1000L;
    
    protected HTablePool hbasePool;
    protected int hbasePoolSize = 20;
    
    protected final byte[] tableName;
    protected final byte[] family;
    protected final byte[] qualifier;
    
    protected boolean prefixDate = true;
    protected int batchSize = 1000;
    protected List<Put> puts;
    
    public HBaseSink(String tableName, String family, String qualifier, boolean prefixDate) {
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        this.prefixDate = prefixDate;
        
        Configuration conf = HBaseConfiguration.create();
        hbasePool = new HTablePool(conf, hbasePoolSize);
        
        puts = new ArrayList<Put>();
    }
    
    public void close() {
        if (hbasePool != null) {
            try {
                flush();
            } catch (IOException e) {
                LOG.error("Error flushing batch in close", e);
            }
            hbasePool.closeTablePool(tableName);
        }
    }

    public void flush() throws IOException {
        HTable table = (HTable) hbasePool.getTable(tableName);
        table.setAutoFlush(false);
        try {
            table.put(puts);
            table.flushCommits();
            // clear puts for next batch
            puts.clear();
        } finally {
            if (hbasePool != null && table != null) {
                hbasePool.putTable(table);
            }
        }
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        Put p = new Put(Bytes.toBytes(key));
        p.add(family, qualifier, data);
        puts.add(p);
        if (puts.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        byte[] k = prefixDate ? IdUtil.bucketizeId(key, timestamp) : Bytes.toBytes(key);
        Put p = new Put(k);
        p.add(family, qualifier, data);
        puts.add(p);
        if (puts.size() >= batchSize) {
            flush();
        }
    }

}
