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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

/**
 * An implementation of Hazelcast's MapStore interface that persists map data to
 * HDFS SequenceFile(s). Due to the general size of data in HDFS there is no interest for this
 * particular implementation to ever load keys. Therefore only the store and
 * storeAll methods are implemented.
 */
public class HdfsMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(HdfsMapStore.class);

    private FileSystem hdfs;
    private Path baseDir;
    private SequenceFile.Writer writer;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private long bytesWritten = 0;
    private SimpleDateFormat sdf;
    private long maxFileSize = 0;
    private long previousRolloverMillis = 0;

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        Configuration conf = new Configuration();
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("hadoop.")) {
                conf.set(name, properties.getProperty(name));
            }
        }

        String hdfsBaseDir = properties.getProperty("hazelcast.hdfs.basedir", "/bagheera");
        String dateFormat = properties.getProperty("hazelcast.hdfs.dateformat", "yyyy-MM-dd");
        sdf = new SimpleDateFormat(dateFormat);
        Calendar cal = Calendar.getInstance();
        if (!hdfsBaseDir.endsWith(Path.SEPARATOR)) {
            baseDir = new Path(hdfsBaseDir + Path.SEPARATOR + mapName + Path.SEPARATOR + sdf.format(cal.getTime()));
        } else {
            baseDir = new Path(hdfsBaseDir + mapName + Path.SEPARATOR + sdf.format(cal.getTime()));
        }

        maxFileSize = Integer.parseInt(properties.getProperty("hazelcast.hdfs.max.filesize","0"));
        LOG.info("Using HDFS max file size: " + maxFileSize);
        previousRolloverMillis = System.currentTimeMillis();

        try {
            hdfs = FileSystem.get(conf);
            initWriter();
        } catch (IOException e) {
            LOG.error("Error initializing SequenceFile.Writer", e);
            throw new RuntimeException(e);
        }
    }

    private void initWriter() throws IOException {
        if (!hdfs.exists(baseDir)) {
            hdfs.mkdirs(baseDir);
        }

        Path outputPath = new Path(baseDir, new Path(UUID.randomUUID().toString()));
        LOG.info("Opening file handle to: " + outputPath.toString());
        writer = new SequenceFile.Writer(hdfs, hdfs.getConf(), outputPath, Text.class, Text.class);
    }

    private void checkRollover() throws IOException {
        boolean getNewFile = false;
        Calendar prev = Calendar.getInstance();
        prev.setTimeInMillis(previousRolloverMillis);
        prev.add(Calendar.DATE, 1);
        Calendar now = Calendar.getInstance();
        if (maxFileSize != 0 && bytesWritten >= maxFileSize) {
            getNewFile = true;
        } else if (now.after(prev)) {
            getNewFile = true;
            baseDir = new Path(baseDir.getParent(), new Path(sdf.format(now.getTime())));
        }

        if (getNewFile) {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            bytesWritten = 0;
            initWriter();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
     */
    public void destroy() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Error closing SequenceFile.Writer", e);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapLoader#load(java.lang.Object)
     */
    @Override
    public String load(String key) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapLoader#loadAll(java.util.Collection)
     */
    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#delete(java.lang.Object)
     */
    @Override
    public void delete(String key) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#deleteAll(java.util.Collection)
     */
    @Override
    public void deleteAll(Collection<String> keys) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.hazelcast.core.MapStore#store(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void store(String key, String value) {
        outputKey.set(key);
        outputValue.set(value);
        try {
            checkRollover();
            bytesWritten += outputKey.getLength() + outputValue.getLength();
            writer.append(outputKey, outputValue);
        } catch (IOException e) {
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

        for (Map.Entry<String, String> pair : pairs.entrySet()) {
            store(pair.getKey(), pair.getValue());
        }
    }

    @Override
    public Set<String> loadAllKeys() {
        // TODO Auto-generated method stub
        return null;
    }
}
