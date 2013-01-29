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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.util.IdUtil;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class HBaseSink implements KeyValueSink {
    
    private static final Logger LOG = Logger.getLogger(HBaseSink.class);

    private static final int DEFAULT_POOL_SIZE = 8;
    
    protected long sleepTime = 1000L;
    
    protected HTablePool hbasePool;
    
    protected final byte[] tableName;
    protected final byte[] family;
    protected final byte[] qualifier;
    
    protected boolean prefixDate = true;
    protected int batchSize = 100;
    
    protected AtomicInteger putsQueueSize = new AtomicInteger();
    protected ConcurrentLinkedQueue<Put> putsQueue = new ConcurrentLinkedQueue<Put>(); 
    
    protected final Meter stored;
    protected final Timer flushTimer;
    
    public HBaseSink(SinkConfiguration sinkConfiguration) {
        this(sinkConfiguration.getString("hbasesink.hbase.tablename"),
             sinkConfiguration.getString("hbasesink.hbase.column.family", "data"),
             sinkConfiguration.getString("hbasesink.hbase.column.qualifier", "json"),
             sinkConfiguration.getBoolean("hbasesink.hbase.rowkey.prefixdate", false),
             sinkConfiguration.getInt("hbasesink.hbase.numthreads", DEFAULT_POOL_SIZE));
    }
    
    public HBaseSink(String tableName, String family, String qualifier, boolean prefixDate, int numThreads) {
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        this.prefixDate = prefixDate;
        
        Configuration conf = HBaseConfiguration.create();
        hbasePool = new HTablePool(conf, numThreads);
        
        stored = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".stored"), "messages", TimeUnit.SECONDS);
        flushTimer = Metrics.newTimer(new MetricName("bagheera", "sink.hbase", tableName + ".flush.time"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }
    
    public void close() {
        if (hbasePool != null) {
            if (!Thread.currentThread().isInterrupted()) {
                try {
                    flush();
                } catch (IOException e) {
                    LOG.error("Error flushing batch in close", e);
                }
            }
            hbasePool.closeTablePool(tableName);
        }
    }

    public void flush() throws IOException {
        HTable table = (HTable) hbasePool.getTable(tableName);
        table.setAutoFlush(false);
        final TimerContext t = flushTimer.time();
        try {
            List<Put> puts = new ArrayList<Put>(batchSize);
            while (!putsQueue.isEmpty() && puts.size() < batchSize) {
                Put p = putsQueue.poll();
                if (p != null) {
                    puts.add(p);
                    putsQueueSize.decrementAndGet();
                }
            }
            table.put(puts);
            table.flushCommits();
            stored.mark(puts.size());
        } finally {
            t.stop();
            if (hbasePool != null && table != null) {
                hbasePool.putTable(table);
            }
        }
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        Put p = new Put(Bytes.toBytes(key));
        p.add(family, qualifier, data);
        putsQueue.add(p);      
        if (putsQueueSize.incrementAndGet() >= batchSize) {
            flush();
        }
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        byte[] k = prefixDate ? IdUtil.bucketizeId(key, timestamp) : Bytes.toBytes(key);
        Put p = new Put(k);
        p.add(family, qualifier, data);
        putsQueue.add(p);
        if (putsQueueSize.incrementAndGet() >= batchSize) {
            flush();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        HTable table = (HTable) hbasePool.getTable(tableName);
        try {
            Delete d = new Delete(Bytes.toBytes(key));
            table.delete(d);
        } finally {
            if (hbasePool != null && table != null) {
                hbasePool.putTable(table);
            }
        }
    }

}
