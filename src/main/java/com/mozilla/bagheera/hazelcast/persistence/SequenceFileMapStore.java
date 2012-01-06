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

import java.io.Closeable;
import java.io.IOException;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
public class SequenceFileMapStore extends HdfsMapStore implements MapStore<String, String>, MapLoaderLifecycleSupport, Closeable {

    private static final Logger LOG = Logger.getLogger(SequenceFileMapStore.class);
    
    private SequenceFile.Writer writer;
    private Text outputKey = new Text();
    private Writable outputValue;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    	super.init(hazelcastInstance, properties, mapName);
    	        
        if (outputFormatType == StoreFormatType.SMILE) {
        	outputValue = new BytesWritable();
        } else {
        	outputValue = new Text();
        }
        
        // register with MapStoreRepository
        MapStoreRepository.addMapStore(mapName, this);
    }

    /**
     * Initialize a SequenceFile.Writer for this persistence class to use
     * 
     * @throws IOException
     */
    private synchronized void initWriter() throws IOException {
        LOG.info("Thread " + Thread.currentThread().getId() + " - initWriter() called");
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

    /**
     * Close the SequenceFile.Writer
     * 
     * @throws IOException
     */
    private synchronized void closeWriter() throws IOException {
        LOG.info("Thread " + Thread.currentThread().getId() + " - closeWriter() called");
        if (writer != null) {
            LOG.info("Thread " + Thread.currentThread().getId() + " - writer.close() called");
            writer.close();
            writer = null;
        }
        bytesWritten = 0;
    }
    
    /**
     * Checks to see if we should rollover to another file. Currently this is either on a daily
     * or byte size basis.
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
    @Override
    public void destroy() {
        LOG.info("Thread " + Thread.currentThread().getId() + " - destroy() called");
        try {
            closeWriter();
        } catch (IOException e) {
            LOG.error("Error closing SequenceFile.Writer", e);
        } 
        
        if (writer != null) {
            LOG.info("Closing HDFS and writer isn't null yet - WTF!");
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
        	outputKey.set(key);
        	bytesWritten += outputKey.getLength();
        	if (outputFormatType == StoreFormatType.SMILE) {
        		byte[] smileBytes = jsonSmileConverter.convertToSmile(value);
        		((BytesWritable)outputValue).set(smileBytes, 0, smileBytes.length);
        		bytesWritten += smileBytes.length;
            } else {
            	((Text)outputValue).set(value);
            	bytesWritten += ((Text)outputValue).getLength();
            }
        	
            checkRollover();
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
        if (LOG.isInfoEnabled()) {
            LOG.info(String.format("Thread %s - storing %d items", Thread.currentThread().getId(), pairs.size()));
        }
        
        long startTime = System.currentTimeMillis();
        try {
            checkRollover();
            for (Map.Entry<String, String> pair : pairs.entrySet()) {
            	outputKey.set(pair.getKey());
            	bytesWritten += outputKey.getLength();
            	if (outputFormatType == StoreFormatType.SMILE) {
            		byte[] smileBytes = jsonSmileConverter.convertToSmile(pair.getValue());
            		((BytesWritable)outputValue).set(smileBytes, 0, smileBytes.length);
            		bytesWritten += smileBytes.length;
                } else {
                	((Text)outputValue).set(pair.getValue());
                	bytesWritten += ((Text)outputValue).getLength();
                }

                writer.append(outputKey, outputValue);
            }
        } catch (IOException e) {
            LOG.error("IOException while writing key/value pair", e);
            throw new RuntimeException(e);
        }
        
        LOG.info(String.format("Thread %s - Stored %d items in %d ms", Thread.currentThread().getId(), pairs.size(), (System.currentTimeMillis() - startTime)));
    }
}