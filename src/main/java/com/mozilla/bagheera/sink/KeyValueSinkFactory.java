/*
 * Copyright 2013 Mozilla Foundation
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class KeyValueSinkFactory implements Closeable {

    private static final Logger LOG = Logger.getLogger(KeyValueSinkFactory.class);
    
    private static KeyValueSinkFactory INSTANCE;
    private Map<String,KeyValueSink> sinkMap;
    private SinkConfiguration sinkConfiguration;
    private Class<?> sinkClazz;
    
    /**
     * @param sinkConfiguration
     */
    private KeyValueSinkFactory(Class<?> sinkClazz, SinkConfiguration sinkConfiguration) {
        this.sinkMap = new HashMap<String,KeyValueSink>();
        this.sinkClazz = sinkClazz;
        this.sinkConfiguration = sinkConfiguration;
    }
    
    /**
     * @param sinkConfiguration
     * @return
     */
    public static KeyValueSinkFactory getInstance(Class<?> sinkClazz, SinkConfiguration sinkConfiguration) {
        if (INSTANCE == null) {
            INSTANCE = new KeyValueSinkFactory(sinkClazz, sinkConfiguration);
        }
        
        return INSTANCE;
    }
    
    /**
     * @param namespace
     * @return
     */
    public KeyValueSink getSink(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("Namespace cannot be null");
        }
        
        if (!sinkMap.containsKey(namespace)) {
            sinkConfiguration.setString("namespace", namespace);
            KeyValueSink sink;
            for (Constructor<?> constructor : sinkClazz.getConstructors()) {
                Class<?>[] paramTypes = constructor.getParameterTypes();
                if (KeyValueSink.class.isAssignableFrom(sinkClazz) && 
                    paramTypes.length == 1 && paramTypes[0] == SinkConfiguration.class) {
                    try {
                        sink = (KeyValueSink)constructor.newInstance(sinkConfiguration);
                        sinkMap.put(namespace, sink);
                    } catch (Exception e) {
                        LOG.error("Error constructing new instance of sink class for namespace: " + namespace, e);
                    }
                }
            }
        }
        return sinkMap.get(namespace);
    }

    /* (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    public void close() throws IOException {
        for (Map.Entry<String,KeyValueSink> entry : sinkMap.entrySet()) {
            try {
                LOG.info("Closing sink for namespace: " + entry.getKey());
                entry.getValue().close();
            } catch (IOException e) {
                LOG.info("Error closing sink for namespace: " + entry.getKey());
            }
        }
    }

}