package com.mozilla.bagheera.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HBaseSink implements KeyValueSink {
    
    private static final Logger LOG = Logger.getLogger(HBaseSink.class);

    protected long sleepTime = 1000L;
    
    protected HTablePool hbasePool;
    protected int hbasePoolSize = 40;
    
    protected final byte[] tableName;
    protected final byte[] family;
    protected final byte[] qualifier;
    
    protected boolean prefixDate = true;
    protected int batchSize = 1000;
    protected List<Put> puts;
    
    public HBaseSink(String tableName, String family, String qualifier, boolean prefixDate) {
        this.tableName = Bytes.toBytes(tableName);
        this.family = Bytes.toBytes(family);
        this.qualifier = Bytes.toBytes(qualifier);
        this.prefixDate = prefixDate;
        
        Configuration conf = HBaseConfiguration.create();
        hbasePool = new HTablePool(conf, hbasePoolSize);
        
        puts = new ArrayList<Put>();
    }
    
    public void close() {
        if (hbasePool != null) {
            try {
                flush();
            } catch (IOException e) {
                LOG.error("Error flushing batch in close", e);
            }
            hbasePool.closeTablePool(tableName);
        }
    }

    public void flush() throws IOException {
        HTable table = (HTable) hbasePool.getTable(tableName);
        table.setAutoFlush(false);
        table.flushCommits();
        try {
            table.put(puts);
            // clear puts for next batch
            puts.clear();
        } finally {
            if (hbasePool != null && table != null) {
                hbasePool.putTable(table);
            }
        }
    }

    @Override
    public void store(byte[] key, byte[] data) throws IOException {
        Put p = new Put(key);
        p.add(family, qualifier, data);
        puts.add(p);
        if (puts.size() >= batchSize) {
            flush();
        }
    }

}
