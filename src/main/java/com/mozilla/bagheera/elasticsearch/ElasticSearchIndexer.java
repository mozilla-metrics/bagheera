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

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.node.Node;

import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;

/**
 * This is a temporary class made to take telemetry data in HBase and index in ElasticSearch. In the future 
 * we will simply use MultiMapStore instead.
 */
public class ElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexer.class);

    private final String indexName;
    private final String tableName;
    private final String columnFamily;
    private final String columnQualifier;

    private ExecutorService pool;
    
    private Node node;
    private Client client;
    private HTablePool hbasePool;
    
    public ElasticSearchIndexer(String indexName, String tableName, String columnFamily, String columnQualifier) {
        this.node = nodeBuilder().loadConfigSettings(true).client(true).node();
        this.client = node.client();
        this.indexName = indexName;
        
        Configuration conf = HBaseConfiguration.create();
        this.hbasePool = new HTablePool(conf, 32);
        
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        
        this.pool = Executors.newFixedThreadPool(8);
    }
    
    public void indexHBaseData(Calendar startCal, Calendar endCal) throws InterruptedException, ExecutionException {
        LOG.info("Entering indexing phase ...");

        // Init scanners to give to the worker threads
        Map<byte[],byte[]> columns = new HashMap<byte[], byte[]>();
        columns.put(columnFamily.getBytes(), columnQualifier.getBytes());
        Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, "yyyyMMdd", columns, 1000, false);
        
        List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
        for (Scan s : scans) {
            tasks.add(new IndexWorker(client, indexName, hbasePool, tableName, columnFamily, columnQualifier, s));
        }
        
        int sum = 0;
        for (Future<Integer> f : pool.invokeAll(tasks)) {
            int count = f.get();
            sum += count;
            LOG.info("Thread finished: " + (count > 0 ? "success" : "failed"));
            if (count <= 0) {
                Thread.currentThread().interrupt();
            }
        }
        
        LOG.info(String.format("Indexed %d documents", sum));
    }
    
    public void close() {
        try {
            if (pool != null) {
                try {
                    pool.shutdown();
                    while (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                        LOG.warn("Waited 30 seconds and pool still isn't shutdown");
                    }
                } catch (InterruptedException e) {
                    pool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            if (hbasePool != null && tableName != null) {
                hbasePool.closeTablePool(tableName);
            }
            if (client != null) {
                client.close();
            }
            if (node != null) {
                node.close();
            }
        }
    }
    
    private class IndexWorker implements Callable<Integer> {

        private static final int BATCH_SIZE = 200;

        private final ObjectMapper jsonMapper = new ObjectMapper();
        
        private Client client;
        private final String indexName;
        
        private final String tableName;
        private final String columnFamily;
        private final String columnQualifier;
        
        private HTablePool hbasePool;
        private Scan scan;

        public IndexWorker(Client client, String indexName, HTablePool hbasePool, String tableName, String columnFamily, String columnQualifier, Scan scan) {
            this.client = client;
            this.indexName = indexName;
            
            this.hbasePool = hbasePool;
            this.tableName = tableName;
            this.columnFamily = columnFamily;
            this.columnQualifier = columnQualifier;
            this.scan = scan;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Integer call() throws Exception {
            int counter = 0;
            
            HTableInterface table = null;
            ResultScanner scanner = null;
            try {                
                table = hbasePool.getTable(this.tableName);
                scanner = table.getScanner(scan);
                Result[] results = null;
                while ((results = scanner.next(BATCH_SIZE)).length > 0) {
                    BulkRequestBuilder brb = client.prepareBulk();
                    for (Result r : results) {
                        String indexString = new String(r.getValue(this.columnFamily.getBytes(), this.columnQualifier.getBytes()));
                        Map<String,Object> values = jsonMapper.readValue(indexString, new TypeReference<Map<String,Object>>() { });
                        String rowId = new String(r.getRow());
    
                        //LOG.info(String.format("Adding rowId: %s", rowId));
                        // Pull the date string out of the rowId into a separate field
                        String d = rowId.substring(1,9);
                        values.put("date", d);
                        rowId = rowId.substring(9);
    
                        // Add histname as a value of itself
                        Map<String, Map<String, Object>> hists = (Map<String, Map<String, Object>>)values.get("histograms");
                        for (Map.Entry<String, Map<String, Object>> hist : hists.entrySet()) {
                            String histName = hist.getKey();
                            Map<String, Object> histValues = hist.getValue();
                            histValues.put("histogram_name", histName);
                        }
                        
                        brb.add(Requests.indexRequest(indexName).type(columnFamily).id(rowId).source(jsonMapper.writeValueAsString(values)));
                    }

                    int numActions = brb.numberOfActions();
                    String threadName = Thread.currentThread().getName();
                    LOG.info(String.format("%s - Indexing batch of size %d", threadName, numActions));
                    BulkResponse response = brb.execute().actionGet(60, TimeUnit.SECONDS);
                    LOG.info(String.format("%s - Bulk request finished in %dms", threadName, response.getTookInMillis()));
                    if (response.hasFailures()) {
                        LOG.error(String.format("%s - Had failures during bulk indexing", threadName));
                        counter = -1;
                        break;
                    }
                    counter += numActions;
                    LOG.info(String.format("%s - Indexed %d documents so far", threadName, counter));
                }
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
                if (hbasePool != null && table != null) {
                    hbasePool.putTable(table);
                }
            }
            
            return counter;
        }
        
    }
    
    public static void main(String[] args) throws InterruptedException, ExecutionException, ParseException {
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

        ElasticSearchIndexer esi = null;
        try {
            esi = new ElasticSearchIndexer("data_telemetry", "telemetry", "data", "json");
            esi.indexHBaseData(startCal, endCal);
        } finally {
            if (esi != null) {
                esi.close();
            }
        }
    }

}
