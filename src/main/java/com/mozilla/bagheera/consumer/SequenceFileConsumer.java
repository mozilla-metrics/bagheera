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
package com.mozilla.bagheera.consumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public abstract class SequenceFileConsumer implements Consumer {

    private static final Logger LOG = Logger.getLogger(SequenceFileConsumer.class);
    
    protected static final long DAY_IN_MILLIS = 86400000L;
    
    protected long sleepTime = 1000L;
    
    // HDFS related member vars
    protected Configuration conf;
    protected FileSystem hdfs;
    protected Path baseDir;
    protected SequenceFile.Writer writer;
    protected Text outputKey = new Text();
    protected Text outputValue = new Text();
    protected long prevRolloverMillis = 0L;
    protected long bytesWritten = 0L;
    protected long maxFileSize = 0L;
    protected SimpleDateFormat sdf;

    public SequenceFileConsumer(String namespace, String baseDirPath, String dateFormat, long maxFileSize) throws IOException {
        LOG.info("Initializing writer for map: " + namespace);
        conf = new Configuration();
        conf.setBoolean("fs.automatic.close", false);
        hdfs = FileSystem.newInstance(conf);
        baseDir = new Path(baseDirPath);
        this.maxFileSize = maxFileSize;
        sdf = new SimpleDateFormat(dateFormat);
        Calendar cal = Calendar.getInstance();
        if (!baseDirPath.endsWith(Path.SEPARATOR)) {
            baseDir = new Path(baseDirPath + Path.SEPARATOR + namespace + Path.SEPARATOR + sdf.format(cal.getTime()));
        } else {
            baseDir = new Path(baseDirPath + namespace + Path.SEPARATOR + sdf.format(cal.getTime()));
        }
    }
    
    protected void initWriter() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - initWriter() called");
        }
        
        if (!hdfs.exists(baseDir)) {
            hdfs.mkdirs(baseDir);
        }

        Path outputPath = new Path(baseDir, new Path(UUID.randomUUID().toString()));
        LOG.info("Opening file handle to: " + outputPath.toString());
        
        writer = SequenceFile.createWriter(hdfs, hdfs.getConf(), outputPath, outputKey.getClass(), outputValue.getClass(), CompressionType.BLOCK);
        
        // Get time in millis at a day resolution
        Calendar prev = Calendar.getInstance();
        prev.set(Calendar.HOUR_OF_DAY, 0);
        prev.set(Calendar.MINUTE, 0);
        prev.set(Calendar.SECOND, 0);
        prev.set(Calendar.MILLISECOND, 0);
        prevRolloverMillis = prev.getTimeInMillis();
    }
    
    protected synchronized void checkRollover() throws IOException {
        boolean getNewFile = false;
        Calendar now = Calendar.getInstance();
        if (maxFileSize != 0 && bytesWritten >= maxFileSize) {
            getNewFile = true;
        } else if (now.getTimeInMillis() > (prevRolloverMillis + DAY_IN_MILLIS)) {
            getNewFile = true;
            baseDir = new Path(baseDir.getParent(), new Path(sdf.format(now.getTime())));
        }

        if (writer == null || getNewFile) {
            closeWriter();
            initWriter();
        }
    }
    
    protected synchronized void closeWriter() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Thread " + Thread.currentThread().getId() + " - closeWriter() called");
        }
        
        if (writer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Thread " + Thread.currentThread().getId() + " - writer.close() called");
            }
            writer.close();
            writer = null;
        }
        bytesWritten = 0;
    }
    
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Error closing writer", e);
            }
        }
        if (hdfs != null) {
            try {
                hdfs.close();
            } catch (IOException e) {
                LOG.error("Error closing HDFS handle", e);
            }
        }
    }
    
    public void poll() {
        // This should be implemented by subclasses depending on the type of 
        // queue or map that is being polled.
    }
}
