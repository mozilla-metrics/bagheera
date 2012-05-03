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
package com.mozilla.bagheera.consumer.hazelcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.mozilla.bagheera.consumer.HBaseConsumer;
import com.mozilla.bagheera.util.IdUtil;

public class HazelcastHBaseConsumer extends HBaseConsumer {

    private static final Logger LOG = Logger.getLogger(HazelcastHBaseConsumer.class);

    private ExecutorService workerPool;
    private int workerPoolSize = 8;
    
    // Hazelcast related member vars
    private IMap<String, String> nsMap;
    
    public HazelcastHBaseConsumer(String namespace, String tableName, String family, String qualifier, boolean prefixDate, 
                                  String hzGroupName, String hzGroupPassword, String[] hzClients) {
        super(namespace, tableName, family, qualifier, prefixDate);
        
        workerPool = Executors.newFixedThreadPool(workerPoolSize);
        
        ClientConfig config = new ClientConfig();
        config.addAddress(hzClients);
        config.getGroupConfig().setName(hzGroupName).setPassword(hzGroupPassword);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        nsMap = client.getMap(namespace);
    }
    
    public void close() {
        if (workerPool != null) {
            workerPool.shutdown();
            try {
                if (workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    workerPool.shutdownNow();
                    if (workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                        LOG.error("Unable to shutdown worker pool");
                    }
                }
            } catch (InterruptedException e) {
                workerPool.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                super.close();
            }
        }
    }
    
    public void poll() {
        try {
            while (true) {
                if (!nsMap.isEmpty()) {
                    try {
                        List<Put> puts = new ArrayList<Put>();
                        for (String k : nsMap.keySet()) {
                            String v = nsMap.remove(k);
                            if (v != null) {
                                try {
                                    byte[] rowId = prefixDate ? IdUtil.bucketizeId(k) : Bytes.toBytes(k);
                                    Put p = new Put(rowId);
                                    p.add(family, qualifier, Bytes.toBytes(v));
                                    puts.add(p);
                                } catch (NumberFormatException e) {
                                    LOG.error("Encountered bad key: " + k, e);
                                }
                                if (puts.size() >= batchSize) break;
                            }
                        }
                        if (puts.size() > 0) {
                            workerPool.submit(new HBaseWorkerThread((HTable)hbasePool.getTable(tableName), puts));
                        }
                    } catch (IOException e) {
                        LOG.error("IOException while writing key/value pair", e);
                        throw new RuntimeException(e);
                    }
                } else {
                    Thread.sleep(sleepTime);
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while polling", e);
        }
    }
    
    private class HBaseWorkerThread implements Callable<Boolean> {
        
        private HTable table;
        private List<Put> puts;
        
        public HBaseWorkerThread(HTable table, List<Put> puts) {
            this.table = table;
            this.puts = puts;
        }
        
        @Override
        public Boolean call() throws Exception {
            try {
                //long startTime = System.currentTimeMillis();
                table.setAutoFlush(false);
                table.put(puts);
                table.flushCommits();
                //LOG.info(String.format("Thread %s - stored %d items in %d ms", Thread.currentThread().getName(), puts.size(), (System.currentTimeMillis() - startTime)));
            } catch (IOException e) {
                LOG.error("IOException while writing key/value pair", e);
            } finally {
                hbasePool.putTable(table);
            }
            
            return true;
        }        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        Options options = new Options();
        Option mapName = new Option("m", "map", true, "Name of map in Hazelcast.");
        mapName.setRequired(true);
        options.addOption(mapName);
        options.addOption(new Option("t", "table", true, "HBase table name."));
        options.addOption(new Option("f", "family", true, "Column family."));
        options.addOption(new Option("q", "qualifier", true, "Column qualifier."));
        options.addOption(new Option("p", "prefixdate", false, "Prefix key with salted date."));
        options.addOption(new Option("gn", "groupname", true, "Hazelcast group name."));
        options.addOption(new Option("gp", "grouppassword", true, "Hazelcast group password."));
        options.addOption(new Option("hzservers", true, "Hazelcast server list."));
        
        CommandLineParser parser = new GnuParser();
        HBaseConsumer consumer = null;
        try {
            CommandLine cmd = parser.parse(options, args);
            consumer = new HazelcastHBaseConsumer(cmd.getOptionValue("map"), cmd.getOptionValue("table", cmd.getOptionValue("map")), cmd.getOptionValue("family","data"), 
                                         cmd.getOptionValue("qualifier","json"), Boolean.parseBoolean(cmd.getOptionValue("prefixdate","true")),
                                         cmd.getOptionValue("groupname","bagheera"), cmd.getOptionValue("grouppassword","bagheera"),
                                         cmd.getOptionValue("hzservers","localhost:5701").split(","));
            consumer.poll();            
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HBaseConsumer", options);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

}
