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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.sink.KeyValueSink;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.util.IdUtil;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class HBaseSink implements KeyValueSink {

    private static final Logger LOG = Logger.getLogger(HBaseSink.class);

    private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    static final int DEFAULT_BATCH_SIZE = 100;

    private int retryCount = 5;
    private int retrySleepSeconds = 30;

    protected HTablePool hbasePool;

    protected final byte[] tableName;
    protected final byte[] family;
    protected final byte[] qualifier;

    protected boolean prefixDate = true;
    protected int batchSize = 100;
    protected long maxKeyValueSize;

    protected AtomicInteger rowQueueSize = new AtomicInteger();
    protected ConcurrentLinkedQueue<Row> rowQueue = new ConcurrentLinkedQueue<Row>();

    protected final Meter stored;
    protected final Meter storeFailed;
    protected final Meter deleted;
    protected final Meter deleteFailed;
    protected final Meter oversized;

    protected final Timer flushTimer;
    protected final Timer htableTimer;

    protected final Gauge<Integer> batchSizeGauge;

    public HBaseSink(SinkConfiguration sinkConfiguration) {
        this(sinkConfiguration.getString("hbasesink.hbase.tablename"),
             sinkConfiguration.getString("hbasesink.hbase.column.family", "data"),
             sinkConfiguration.getString("hbasesink.hbase.column.qualifier", "json"),
             sinkConfiguration.getBoolean("hbasesink.hbase.rowkey.prefixdate", false),
             sinkConfiguration.getInt("hbasesink.hbase.numthreads", DEFAULT_POOL_SIZE),
             sinkConfiguration.getInt("hbasesink.hbase.batchsize", DEFAULT_BATCH_SIZE));
    }

    public HBaseSink(String tableName, String family, String qualifier, boolean prefixDate, int numThreads, final int batchSize) {
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        this.prefixDate = prefixDate;
        this.batchSize = batchSize;

        Configuration conf = HBaseConfiguration.create();

        // Use the standard HBase default
        maxKeyValueSize = conf.getLong("hbase.client.keyvalue.maxsize", 10485760l);
        hbasePool = new HTablePool(conf, numThreads);

        stored = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".stored"), "messages", TimeUnit.SECONDS);
        storeFailed = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".store.failed"), "messages", TimeUnit.SECONDS);
        deleted = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".deleted"), "messages", TimeUnit.SECONDS);
        deleteFailed = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".delete.failed"), "messages", TimeUnit.SECONDS);
        oversized = Metrics.newMeter(new MetricName("bagheera", "sink.hbase", tableName + ".oversized"), "messages", TimeUnit.SECONDS);
        flushTimer = Metrics.newTimer(new MetricName("bagheera", "sink.hbase", tableName + ".flush.time"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        htableTimer = Metrics.newTimer(new MetricName("bagheera", "sink.hbase", tableName + ".htable.time"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        batchSizeGauge = Metrics.newGauge(new MetricName("bagheera", "sink.hbase", tableName + ".batchsize"), new Gauge<Integer>(){
            @Override
            public Integer value() {
                return batchSize;
            }
        });
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
            try {
                hbasePool.closeTablePool(tableName);
            } catch(IOException e) {
                LOG.error("Closing hbasePool error", e);
            }
        }
    }


    public void flush() throws IOException {
        IOException lastException = null;
        int i;
        for (i = 0; i < getRetryCount(); i++) {
            HTableInterface table = hbasePool.getTable(tableName);
            try {
                table.setAutoFlush(false);
                final TimerContext flushTimerContext = flushTimer.time();
                try {
                    List<Row> rows = new ArrayList<Row>(batchSize);
                    while (!rowQueue.isEmpty() && rows.size() < batchSize) {
                        Row row = rowQueue.poll();
                        if (row != null) {
                            rows.add(row);
                            rowQueueSize.decrementAndGet();
                        }
                    }
                    try {
                        FlushResult result = flushTable(table, rows);
                        stored.mark(result.successfulPutCount);
                        storeFailed.mark(result.failedPutCount);
                        deleted.mark(result.successfulDeleteCount);
                        deleteFailed.mark(result.failedDeleteCount);
                    } catch (InterruptedException e) {
                        LOG.error("Error flushing batch of " + batchSize + " messages", e);
                    }
                } finally {
                    flushTimerContext.stop();
                    if ( table != null) {
                        table.close();
                    }
                }
                break;
            } catch (IOException e) {
                LOG.warn(String.format("Error in flush attempt %d of %d, clearing Region cache", (i+1), getRetryCount()), e);
                lastException = e;
                // TODO: Clear the region cache.  Is this the correct way?
//                HConnection connection = HConnectionManager.getConnection(table.getConfiguration());
//                connection.clearRegionCache();
                try {
                    Thread.sleep(getRetrySleepSeconds() * 1000);
                } catch (InterruptedException e1) {
                    // wake up
                    LOG.info("woke up by interruption", e1);
                }
            }
        }
        if (i >= getRetryCount() && lastException != null) {
            LOG.error("Error in final flush attempt, giving up.");
            throw lastException;
        }
        LOG.debug("Flush finished");
    }

    private FlushResult flushTable(HTableInterface table, List<Row> puts) throws IOException, InterruptedException {
        List<Row> currentAttempt = puts;
        Object[] batch = null;
        FlushResult result = null;

        int successfulPuts = 0;
        int successfulDeletes = 0;

        TimerContext htableTimerContext = htableTimer.time();
        try {
            for (int attempt = 0; attempt < retryCount; attempt++) {
                // TODO: wrap each attempt in a try/catch?
                batch = table.batch(currentAttempt);
                table.flushCommits();
                List<Row> fails = new ArrayList<Row>(currentAttempt.size());
                if (batch != null) {
                    for (int i = 0; i < batch.length; i++) {
                        if (batch[i] == null) {
                            fails.add(currentAttempt.get(i));
                        } else {
                            // figure out what type it was
                            Row row = currentAttempt.get(i);
                            if (row instanceof Delete) {
                                successfulDeletes++;
                            } else if (row instanceof Put) {
                                successfulPuts++;
                            } else {
                                LOG.warn("We succeeded in flushing something that's neither a Delete nor a Put");
                            }
                        }
                    }

                    currentAttempt = fails;
                    if (currentAttempt.isEmpty()) {
                        break;
                    }
                } else {
                    // something badly broke, retry the whole list.
                    LOG.error("Result of table.batch() was null");
                }
            }

            int failedPuts = 0;
            int failedDeletes = 0;
            if (!currentAttempt.isEmpty()) {
                for (Row row : currentAttempt) {
                    if (row instanceof Delete) {
                        failedDeletes++;
                    } else if (row instanceof Put) {
                        failedPuts++;
                    } else {
                        LOG.error("We failed to flush something that's neither a Delete nor a Put");
                    }
                }
            }

            result = new FlushResult(failedPuts, failedDeletes, successfulPuts, successfulDeletes);
        } finally {
            htableTimerContext.stop();
        }

        return result;
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        if (!isOversized(key, data)) {
            Put p = new Put(Bytes.toBytes(key));
            p.add(family, qualifier, data);
            rowQueue.add(p);
            if (rowQueueSize.incrementAndGet() >= batchSize) {
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
            rowQueue.add(p);
            if (rowQueueSize.incrementAndGet() >= batchSize) {
                flush();
            }
        }
    }

    @Override
    public void delete(String key) throws IOException {
        Delete d = new Delete(Bytes.toBytes(key));
        LOG.info(this.tableName+" CONSUMER_DELETE "+key);
        rowQueue.add(d);
        if (rowQueueSize.incrementAndGet() >= batchSize) {
            flush();
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

class FlushResult {
    public final int failedPutCount;
    public final int failedDeleteCount;
    public final int successfulPutCount;
    public final int successfulDeleteCount;

    public FlushResult(int failedPuts, int failedDeletes, int successfulPuts, int successfulDeletes) {
        this.failedPutCount = failedPuts;
        this.failedDeleteCount = failedDeletes;
        this.successfulPutCount = successfulPuts;
        this.successfulDeleteCount = successfulDeletes;
    }
}
