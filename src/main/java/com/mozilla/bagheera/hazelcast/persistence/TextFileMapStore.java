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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

/**
 * An implementation of Hazelcast's MapStore interface that persists map data to
 * HDFS as raw TextFile(s). Due to the general size of data in HDFS there is no
 * interest for this particular implementation to ever load keys. Therefore only
 * the store and storeAll methods are implemented.
 */
public class TextFileMapStore extends HdfsMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport, Closeable {

    private static final Logger LOG = Logger.getLogger(TextFileMapStore.class);
    
    private BufferedWriter writer;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        super.init(hazelcastInstance, properties, mapName);
    }

    /**
     * @throws IOException
     */
    private synchronized void initWriter() throws IOException {
        if (!hdfs.exists(baseDir)) {
            hdfs.mkdirs(baseDir);
        }

        Path outputPath = new Path(baseDir, new Path(UUID.randomUUID().toString()));
        LOG.info("Opening file handle to: " + outputPath.toString());
        writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(outputPath, true)));
        
        // Get time in millis at a day resolution
        Calendar prev = Calendar.getInstance();
        prev.set(Calendar.HOUR_OF_DAY, 0);
        prev.set(Calendar.MINUTE, 0);
        prev.set(Calendar.SECOND, 0);
        prev.set(Calendar.MILLISECOND, 0);
        prevRolloverMillis = prev.getTimeInMillis();
    }

    /**
     * @throws IOException
     */
    private synchronized void closeWriter() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
        bytesWritten = 0;
    }
    
    /**
     * @throws IOException
     */
    private synchronized void checkRollover() throws IOException {
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

    /* (non-Javadoc)
     * For close we only want to close the file and not the underlying FileSystem. This allows
     * us to close the files regardless of rollover conditions.
     * @see java.io.Closeable#close()
     */
    public void close() throws IOException {
        closeWriter();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
     */
    public void destroy() {
        try {
            closeWriter();
        } catch (IOException e) {
            LOG.error("Error closing writer", e);
        }
        
        closeHDFS();
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
            checkRollover();
            writer.append(value);
            writer.newLine();
            bytesWritten += value.length();
            isHealthy = true;
            metricsManager.getHazelcastMetricForNamespace(mapName).updateStoreMetrics(1, true);
        } catch (IOException e) {
            isHealthy = false;
            metricsManager.getHazelcastMetricForNamespace(mapName).updateStoreMetrics(0, false);
            LOG.error("IOException while writing key/value pair", e);
            throw new RuntimeException(e);
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

        int numWritten = 0;
        try {
            checkRollover();
            for (Map.Entry<String, String> pair : pairs.entrySet()) {
                writer.append(pair.getValue());
                writer.newLine();
                bytesWritten += pair.getValue().length();
                numWritten++;
            }
            isHealthy = true;
            metricsManager.getHazelcastMetricForNamespace(mapName).updateStoreMetrics(numWritten, true);
        } catch (IOException e) {
            isHealthy = false;
            metricsManager.getHazelcastMetricForNamespace(mapName).updateStoreMetrics(numWritten, false);
            LOG.error("IOException while writing key/value pair", e);
            throw new RuntimeException(e);
        }
    }

}
