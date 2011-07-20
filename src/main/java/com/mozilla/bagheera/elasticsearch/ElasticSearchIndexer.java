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
package com.mozilla.bagheera.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.mozilla.bagheera.dao.ElasticSearchDao;
import com.mozilla.bagheera.dao.HBaseTableDao;

public class ElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexer.class);
    
    private HBaseTableDao hbaseTableDao;
    private ElasticSearchDao es;
    
    public ElasticSearchIndexer(HBaseTableDao hbaseTableDao, ElasticSearchDao es) {
        this.hbaseTableDao = hbaseTableDao;
        this.es = es;
    }
    
    public void indexHBaseData(HTablePool pool) {
        HTableInterface table = pool.getTable(hbaseTableDao.getTableName());
        Scan s = new Scan();
        s.addColumn(hbaseTableDao.getColumnFamily(), hbaseTableDao.getColumnQualifier());
        ResultScanner scanner;
        try {
            scanner = table.getScanner(s);
            Map<String,String> batch = new HashMap<String,String>();
            for (Result r : scanner) {
                String indexString = new String(r.getValue(hbaseTableDao.getColumnFamily(), hbaseTableDao.getColumnQualifier()));
                String rowId = new String(r.getRow());
                LOG.info("Adding row to batch: " + rowId);
                batch.put(rowId, indexString);
                
                if (batch.size() == 100) {
                    LOG.info("Indexing batch...");
                    es.indexDocuments(batch);
                    batch.clear();
                }
            }
            
            if (!batch.isEmpty()) {
                LOG.info("Indexing batch...");
                es.indexDocuments(batch);
            }
        } catch (IOException e) {
            LOG.error("IO error occurred during indexing", e);
        } finally {
            pool.putTable(table);
        }
    }
    
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTablePool pool = new HTablePool(conf, 20);
        HBaseTableDao table = new HBaseTableDao(pool, "telemetry", "data", "json");
        Node node = null;
        Client client = null;
        try {
            node = NodeBuilder.nodeBuilder().client(true).node();
            client = node.client();
            ElasticSearchDao es = new ElasticSearchDao(client, "telemetry", "data");
            ElasticSearchIndexer esi = new ElasticSearchIndexer(table, es);
            esi.indexHBaseData(pool);
        } finally {
            if (pool != null && table != null && table.getTableName() != null) {
                pool.closeTablePool(table.getTableName());
            }
            if (client != null) {
                client.close();
            }
            if (node != null) {
                node.close();
            }
        }
    }
    
}
