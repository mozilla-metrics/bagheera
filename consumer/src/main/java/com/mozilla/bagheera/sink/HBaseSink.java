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
import org.apache.hadoop.hbase.HRegionLocation;
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

    private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_HBASE_RETRIES = 5;
    private static final int DEFAULT_HBASE_RETRY_SLEEP_SECONDS = 30;

    protected HTablePool hbasePool;

    protected final byte[] tableName;
    protected final byte[] family;
    protected final byte[] qualifier;

    protected boolean prefixDate = true;
    protected int batchSize = 100;
    protected long maxKeyValueSize;

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
        LOG.info("Before Bytes.toBytes");
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        this.prefixDate = prefixDate;

        Configuration conf = HBaseConfiguration.create();

        // Use the standard HBase default
        maxKeyValueSize = conf.getLong("hbase.client.keyvalue.maxsize", 10485760l);
        hbasePool = new HTablePool(conf, numThreads);

        stored = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".stored"), "messages", TimeUnit.SECONDS);
        flushTimer = Metrics.newTimer(new MetricName("bagheera", "sink.hbase", tableName + ".flush.time"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }

    @Override
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
        IOException lastException = null;
        int i;
        for (i = 0; i < DEFAULT_HBASE_RETRIES; i++) {
            LOG.info(String.format("Starting flush attempt %d of %d", (i+1), DEFAULT_HBASE_RETRIES));
            HTable table = (HTable) hbasePool.getTable(tableName);
            try {
                table.setAutoFlush(false);
                final TimerContext t = flushTimer.time();
                try {
                    LOG.info("Getting up to " + batchSize + " Puts");
                    List<Put> puts = new ArrayList<Put>(batchSize);
                    while (!putsQueue.isEmpty() && puts.size() < batchSize) {
                        Put p = putsQueue.poll();
                        if (p != null) {
                            HRegionLocation regionLocation = table.getRegionLocation(p.getRow());
                            LOG.info("row served by " + regionLocation.getServerAddress().getHostname());
                            puts.add(p);
                            putsQueueSize.decrementAndGet();
                        }
                    }
                    LOG.info("Putting Puts");
                    table.put(puts);
                    LOG.info("Flushing commits");
                    table.flushCommits();
                    LOG.info("Marking committed Puts");
                    stored.mark(puts.size());
                } finally {
                    LOG.info("Stopping timer");
                    t.stop();
                    if (hbasePool != null && table != null) {
                        LOG.info("calling putTable");
                        hbasePool.putTable(table);
                    }
                }
                LOG.info(String.format("Flush succeeded on attempt %d of %d", (i+1), DEFAULT_HBASE_RETRIES));
                break;
            } catch (IOException e) {
                LOG.warn(String.format("Error in flush attempt %d of %d", (i+1), DEFAULT_HBASE_RETRIES), e);
                lastException = e;
                LOG.info("clearing Region cache");
                table.clearRegionCache();
                LOG.info("sleeping...");
                try {
                    Thread.sleep(DEFAULT_HBASE_RETRY_SLEEP_SECONDS * 1000);
                } catch (InterruptedException e1) {
                    // wake up
                    LOG.info("woke up by interruption", e1);
                }
                LOG.info("woke up");
            }
        }
        if (i >= DEFAULT_HBASE_RETRIES && lastException != null) {
            LOG.error("Error in final flush attempt, giving up.");
            throw lastException;
        }
        LOG.info("Flush finished");
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        // There's a max size for 'data', exceeding causes
        //   java.lang.IllegalArgumentException: KeyValue size too large
        // Detect, log, and reject it.
        if (data != null && data.length > maxKeyValueSize) {
            LOG.warn(String.format("Storing key '%s': Data exceeds max length (%d > %d)",
                    key, data.length, maxKeyValueSize));
            return;
        }

        Put p = new Put(Bytes.toBytes(key));
        p.add(family, qualifier, data);
        putsQueue.add(p);
        if (putsQueueSize.incrementAndGet() >= batchSize) {
            flush();
        }
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        // There's a max size for 'data', exceeding causes
        //   java.lang.IllegalArgumentException: KeyValue size too large
        // Detect, log, and reject it.
        if (data != null && data.length > maxKeyValueSize) {
            LOG.warn(String.format("Storing key '%s': Data exceeds max length (%d > %d)",
                    key, data.length, maxKeyValueSize));
            return;
        }

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
