package com.mozilla.bagheera.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.mozilla.bagheera.util.IdUtil;

public class HBaseConsumer {

    private static final Logger LOG = Logger.getLogger(HBaseConsumer.class);

    private long sleepTime = 1000L;
    
    private ExecutorService workerPool;
    private int workerPoolSize = 8;
    
    private HTablePool hbasePool;
    private int hbasePoolSize = 40;
    
    private final byte[] tableName;
    private final byte[] family;
    private final byte[] qualifier;
    
    private boolean prefixDate = true;
    private int batchSize = 1000;
    
    // Hazelcast related member vars
    private IMap<String, String> nsMap;
    
    public HBaseConsumer(String mapName, String tableName, String family, String qualifier, boolean prefixDate, 
                         String hzGroupName, String hzGroupPassword, String[] hzClients) {
        workerPool = Executors.newFixedThreadPool(workerPoolSize);
        
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        Configuration conf = HBaseConfiguration.create();
        hbasePool = new HTablePool(conf, hbasePoolSize);
        this.prefixDate = prefixDate;
        
        HazelcastInstance client = HazelcastClient.newHazelcastClient(hzGroupName, hzGroupPassword, hzClients);
        nsMap = client.getMap(mapName);
    }
    
    public void close() {
        if (hbasePool != null) {
            hbasePool.closeTablePool(tableName);
        }
    }
    
    public void poll() throws InterruptedException {
        while (true) {
            if (!nsMap.isEmpty()) {
                try {
                    List<Put> puts = new ArrayList<Put>();
                    for (String k : nsMap.keySet()) {
                        String v = nsMap.remove(k);
                        if (v != null) {
                            byte[] rowId = prefixDate ? IdUtil.bucketizeId(k) : Bytes.toBytes(k);
                            Put p = new Put(rowId);
                            p.add(family, qualifier, Bytes.toBytes(v));
                            puts.add(p);
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
            consumer = new HBaseConsumer(cmd.getOptionValue("map"), cmd.getOptionValue("table", cmd.getOptionValue("map")), cmd.getOptionValue("family","data"), 
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
