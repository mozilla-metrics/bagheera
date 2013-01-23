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

import org.apache.log4j.Logger;

public class LoggerSink implements Sink, KeyValueSink {

    private static final Logger LOG = Logger.getLogger(LoggerSink.class);
    
    private boolean logValues;
    
    public LoggerSink(SinkConfiguration sinkConfiguration) {
        this(sinkConfiguration.getBoolean("loggersink.logvalues", false));
    }
    
    public LoggerSink(boolean logValues) {
        this.logValues = logValues;
    }
    
    @Override
    public void close() {
        LOG.info("Called close()");
    }
    
    @Override
    public void store(byte[] data) throws IOException {
        LOG.info("data length:" + data.length);
        if (logValues) {
            LOG.info("data: " + new String(data, "UTF-8"));
        }
    }
    
    @Override
    public void store(String key, byte[] data) throws IOException {
        LOG.info("Called store(key,data)");
        LOG.info("key length: " + key.length());
        LOG.info("data length:" + data.length);
        if (logValues) {
            LOG.info("key: " + key);
            LOG.info("data: " + new String(data, "UTF-8"));
        }
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        LOG.info("Called store(key,data,timestamp)");
        LOG.info("key length: " + key.length());
        LOG.info("data length:" + data.length);
        LOG.info("timestamp: " + timestamp);
        if (logValues) {
            LOG.info("key: " + key);
            LOG.info("data: " + new String(data, "UTF-8"));
        }    
    }

    @Override
    public void delete(String key) {
        LOG.info("Called delete(key)");
        LOG.info("key length: " + key.length());
        if (logValues) {
            LOG.info("key: " + key);
        }
    }

}
