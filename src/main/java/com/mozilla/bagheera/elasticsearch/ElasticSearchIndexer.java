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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.mozilla.bagheera.dao.ElasticSearchDao;
import com.mozilla.bagheera.dao.HBaseTableDao;
import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;

/**
 * This is a temporary class made to take telemetry data in HBase and index in ElasticSearch. In the future 
 * we will simply use MultiMapStore instead.
 */
public class ElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexer.class);

    private final HBaseTableDao hbaseTableDao;
    private final ElasticSearchDao es;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public ElasticSearchIndexer(HBaseTableDao hbaseTableDao, ElasticSearchDao es) {
        this.hbaseTableDao = hbaseTableDao;
        this.es = es;
    }
    
    public void indexHBaseData(HTablePool pool, Calendar startCal, Calendar endCal) {
        LOG.info("Entering indexing phase ...");
        HTableInterface table = pool.getTable(hbaseTableDao.getTableName());
        Map<byte[],byte[]> columns = new HashMap<byte[], byte[]>();
        columns.put(hbaseTableDao.getColumnFamily(), hbaseTableDao.getColumnQualifier());
        Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, "yyyyMMdd", columns, 100, false);
        try {
            ResultScanner scanner = null;
            for (Scan s : scans) {
                try {
                    scanner = table.getScanner(s);
                    Map<String,String> batch = new HashMap<String,String>();
                    for (Result r : scanner) {
                        String indexString = new String(r.getValue(hbaseTableDao.getColumnFamily(), hbaseTableDao.getColumnQualifier()));
                        Map<String,Object> values = jsonMapper.readValue(indexString, new TypeReference<Map<String,Object>>() { });
                        String rowId = new String(r.getRow());

                        // Pull the date string out of the rowId into a separate field
                        String d = rowId.substring(1,9);
                        values.put("date", d);
                        rowId = rowId.substring(9);

                        LOG.info("Adding row to batch: " + rowId);
                        batch.put(rowId, jsonMapper.writeValueAsString(values));

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
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
            }

        } catch (IOException e) {
            LOG.error("IO error occurred during indexing", e);
        } finally {
            pool.putTable(table);
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        // Setup the start and stop dates for scanning
        Calendar startCal = Calendar.getInstance();
        Calendar endCal = Calendar.getInstance();
        if (args.length == 2) {
            String startDateStr = args[0];
            String endDateStr = args[1];
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            startCal.setTime(sdf.parse(startDateStr));
            endCal.setTime(sdf.parse(endDateStr));
        }

        HTablePool pool = null;
        HBaseTableDao table = null;
        Node node = null;
        Client client = null;
        try {
            Configuration conf = HBaseConfiguration.create();
            LOG.info("HDFS Namnode: " + conf.get("fs.default.name"));
            pool = new HTablePool(conf, 20);
            table = new HBaseTableDao(pool, "telemetry", "data", "json", true);
            node = NodeBuilder.nodeBuilder().loadConfigSettings(true).node();
            LOG.info("ES Cluster Name: " + node.settings().get("cluster.name"));
            client = node.client();
            ElasticSearchDao es = new ElasticSearchDao(client, "telemetry", "data");
            ElasticSearchIndexer esi = new ElasticSearchIndexer(table, es);
            esi.indexHBaseData(pool, startCal, endCal);
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
