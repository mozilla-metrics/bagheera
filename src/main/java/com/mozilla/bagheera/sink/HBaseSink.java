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

    private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    private int retryCount = 5;
    private int retrySleepSeconds = 30;

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
    protected final Meter deleted;
    protected final Meter oversized;

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

        // Use the standard HBase default
        maxKeyValueSize = conf.getLong("hbase.client.keyvalue.maxsize", 10485760l);
        hbasePool = new HTablePool(conf, numThreads);

        stored = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".stored"), "messages", TimeUnit.SECONDS);
        deleted = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".deleted"), "messages", TimeUnit.SECONDS);
        oversized = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".oversized"), "messages", TimeUnit.SECONDS);
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
        for (i = 0; i < getRetryCount(); i++) {
            HTable table = (HTable) hbasePool.getTable(tableName);
            try {
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
                break;
            } catch (IOException e) {
                LOG.warn(String.format("Error in flush attempt %d of %d, clearing Region cache", (i+1), getRetryCount()), e);
                lastException = e;
                table.clearRegionCache();
                try {
                    Thread.sleep(getRetrySleepSeconds() * 1000);
                } catch (InterruptedException e1) {
                    // wake up
                    LOG.info("Sleep interrupted during retry", e);
                }
            }
        }
        if (i >= getRetryCount() && lastException != null) {
            LOG.error("Error in final flush attempt, giving up (rethrowing exception)", lastException);
            throw lastException;
        }
        LOG.debug("Flush finished");
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        if (!isOversized(key, data)) {
            Put p = new Put(Bytes.toBytes(key));
            p.add(family, qualifier, data);
            putsQueue.add(p);
            if (putsQueueSize.incrementAndGet() >= batchSize) {
                flush();
            }
        }
    }

    // There is a max size for 'data', exceeding it causes
    //   java.lang.IllegalArgumentException: KeyValue size too large
    // Detect, log, and reject it.
    private boolean isOversized(String key, byte[] data) {
        boolean tooBig = false;
        if (data != null && data.length > maxKeyValueSize) {
            LOG.warn(String.format("Storing key '%s': Data exceeds max length (%d > %d)",
                    key, data.length, maxKeyValueSize));
            oversized.mark();
            tooBig = true;
        }
        return tooBig;
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        if (!isOversized(key, data)) {
            byte[] k = prefixDate ? IdUtil.bucketizeId(key, timestamp) : Bytes.toBytes(key);
            Put p = new Put(k);
            p.add(family, qualifier, data);
            putsQueue.add(p);
            if (putsQueueSize.incrementAndGet() >= batchSize) {
                flush();
            }
        }
    }

    @Override
    public void delete(String key) throws IOException {
        HTable table = (HTable) hbasePool.getTable(tableName);
        try {
            Delete d = new Delete(Bytes.toBytes(key));
            table.delete(d);
            deleted.mark();
        } finally {
            if (hbasePool != null && table != null) {
                hbasePool.putTable(table);
            }
        }
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getRetrySleepSeconds() {
        return retrySleepSeconds;
    }

    public void setRetrySleepSeconds(int retrySleepSeconds) {
        this.retrySleepSeconds = retrySleepSeconds;
    }
}
