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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class SequenceFileSink implements KeyValueSink {

    private static final Logger LOG = Logger.getLogger(SequenceFileSink.class);

    protected static final long DAY_IN_MILLIS = 86400000L;

    protected long sleepTime = 1000L;

    // HDFS related member vars
    protected final Semaphore lock = new Semaphore(1, true);
    protected final Configuration conf;
    protected final FileSystem hdfs;
    protected SequenceFile.Writer writer;
    protected Path baseDir;
    protected Path outputPath;
    protected boolean useBytesValue;
    protected long nextRolloverMillis = 0L;
    protected AtomicLong bytesWritten = new AtomicLong();
    protected long maxFileSize = 0L;
    protected final SimpleDateFormat sdf;

    protected Meter stored;

    public SequenceFileSink(String namespace, String baseDirPath, String dateFormat, long maxFileSize,
            boolean useBytesValue) throws IOException {
        LOG.info("Initializing writer for namespace: " + namespace);
        conf = new Configuration();
        conf.setBoolean("fs.automatic.close", false);
        hdfs = FileSystem.newInstance(conf);
        this.useBytesValue = useBytesValue;
        this.maxFileSize = maxFileSize;
        sdf = new SimpleDateFormat(dateFormat);
        if (!baseDirPath.endsWith(Path.SEPARATOR)) {
            baseDir = new Path(baseDirPath + Path.SEPARATOR + namespace + Path.SEPARATOR + 
                               sdf.format(new Date(System.currentTimeMillis())));
        } else {
            baseDir = new Path(baseDirPath + namespace + Path.SEPARATOR + 
                               sdf.format(new Date(System.currentTimeMillis())));
        }
        initWriter();
        stored = Metrics.newMeter(new MetricName("bagheera", "sink.hdfs.", namespace + ".stored"), "messages",
                TimeUnit.SECONDS);
    }

    private void initWriter() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - initWriter() called");
        }

        if (!hdfs.exists(baseDir)) {
            hdfs.mkdirs(baseDir);
        }

        outputPath = new Path(baseDir, new Path(UUID.randomUUID().toString()));
        LOG.info("Opening file handle to: " + outputPath.toString());

        if (useBytesValue) {
            writer = SequenceFile.createWriter(hdfs, conf, outputPath, Text.class, BytesWritable.class,
                    CompressionType.BLOCK);
        } else {
            writer = SequenceFile.createWriter(hdfs, conf, outputPath, Text.class, Text.class, CompressionType.BLOCK);
        }

        // Get time in millis at a day resolution
        Calendar prev = Calendar.getInstance();
        prev.set(Calendar.HOUR_OF_DAY, 0);
        prev.set(Calendar.MINUTE, 0);
        prev.set(Calendar.SECOND, 0);
        prev.set(Calendar.MILLISECOND, 0);
        nextRolloverMillis = prev.getTimeInMillis() + DAY_IN_MILLIS;
    }

    private void checkRollover() throws IOException {
        boolean getNewFile = false;
        long now = System.currentTimeMillis();
        if (maxFileSize != 0 && bytesWritten.get() >= maxFileSize) {
            getNewFile = true;
        } else if (now > nextRolloverMillis) {
            getNewFile = true;
            baseDir = new Path(baseDir.getParent(), new Path(sdf.format(new Date(now))));
        }

        if (writer == null || getNewFile) {
            closeWriter();
            initWriter();
        }
    }

    private void closeWriter() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
        bytesWritten.set(0);
    }

    public void close() {
        try {
            lock.acquire();
            LOG.info("Closing file handle to: " + outputPath.toString());
            try {
                closeWriter();
            } catch (IOException e) {
                LOG.error("Error closing writer", e);
            }

            if (hdfs != null) {
                try {
                    LOG.info("fs.automatic.close = " + hdfs.getConf().get("fs.automatic.close"));
                    hdfs.close();
                } catch (IOException e) {
                    LOG.error("Error closing HDFS handle", e);
                }
            }
        } catch (InterruptedException ex) {
            LOG.error("Interrupted while closing HDFS handle", ex);
        } finally {
            lock.release();
        }
    }

    @Override
    public void store(String key, byte[] data) {
        try {
            lock.acquire();
            checkRollover();
            if (useBytesValue) {
                writer.append(new Text(key), new BytesWritable(data));
            } else {
                writer.append(new Text(key), new Text(data));
            }
            stored.mark();
            bytesWritten.getAndAdd(key.length() + data.length);
        } catch (IOException e) {
            LOG.error("IOException while writing key/value pair", e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while writing key/value pair", e);
        } finally {
            lock.release();
        }
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        // HDFS sink will currently ignore timestamps
        store(key, data);
    }

    @Override
    public void delete(String key) {
        // TODO: Throw error or just ignore?
        // NOOP
    }

}
