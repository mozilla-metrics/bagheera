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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public abstract class HBaseConsumer implements Consumer {

    private static final Logger LOG = Logger.getLogger(HBaseConsumer.class);

    protected long sleepTime = 1000L;
    
    protected HTablePool hbasePool;
    protected int hbasePoolSize = 40;
    
    protected final byte[] tableName;
    protected final byte[] family;
    protected final byte[] qualifier;
    
    protected boolean prefixDate = true;
    protected int batchSize = 1000;
    
    public HBaseConsumer(String namespace, String tableName, String family, String qualifier, boolean prefixDate) {
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        Configuration conf = HBaseConfiguration.create();
        hbasePool = new HTablePool(conf, hbasePoolSize);
        this.prefixDate = prefixDate;
    }
    
    public void close() {
        if (hbasePool != null) {
            hbasePool.closeTablePool(tableName);
        }
    }
    
    public void poll() {
    }
}
