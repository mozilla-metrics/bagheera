package com.mozilla.bagheera.hazelcast.persistence;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.mozilla.bagheera.dao.ElasticSearchDao;
import com.mozilla.bagheera.dao.HBaseTableDao;
import com.mozilla.bagheera.elasticsearch.ClientFactory;

public abstract class ComplexMapStoreBase implements MapLoaderLifecycleSupport {

    private static final Logger LOG = Logger.getLogger(ComplexMapStoreBase.class);
    
    protected boolean allowLoad = false;
    protected boolean allowLoadAll = false;
    protected boolean allowDelete = false;
    
    protected String mapName;
    
    protected HTablePool pool;
    protected HBaseTableDao table;
    
    protected Client esClient;
    protected ElasticSearchDao es;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
     * HazelcastInstance, java.util.Properties, java.lang.String)
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.mapName = mapName;
        this.initHBase(hazelcastInstance, properties, mapName);
        this.initElasticSearch(hazelcastInstance, properties, mapName);
    }
    
    /**
     * @param hazelcastInstance
     * @param properties
     * @param mapName
     */
    protected void initHBase(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        Configuration conf = HBaseConfiguration.create();
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("hbase.") || name.startsWith("hadoop.") || name.startsWith("zookeeper.")) {
                conf.set(name, properties.getProperty(name));
            }
        }

        int hbasePoolSize = Integer.parseInt(properties.getProperty("hazelcast.hbase.pool.size", "10"));
        String tableName = properties.getProperty("hazelcast.hbase.table", "default");
        String family = properties.getProperty("hazelcast.hbase.column.family", "data");
        String columnQualifier = properties.getProperty("hazelcast.hbase.column.qualifier");
        String qualifier = columnQualifier == null ? "" : columnQualifier;

        pool = new HTablePool(conf, hbasePoolSize);
        table = new HBaseTableDao(pool, tableName, family, qualifier);
    }
    
    /**
     * @param hazelcastInstance
     * @param properties
     * @param mapName
     */
    protected void initElasticSearch(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        String indexName = properties.getProperty("hazelcast.elasticsearch.index", "default");
        String typeName = properties.getProperty("hazelcast.elasticsearch.type.name", "data");
        esClient = ClientFactory.getInstance().getNodeClient(mapName, properties, true);
        es = new ElasticSearchDao(esClient, indexName, typeName);
    }
    
    /* (non-Javadoc)
     * @see com.hazelcast.core.MapLoaderLifecycleSupport#destroy()
     */
    public void destroy() {
        if (pool != null) {
            pool.closeTablePool(table.getTableName());
        }
        
        ClientFactory.getInstance().close(mapName);
    }

    /**
     * @param row
     */
    public void fetchAndIndex(String row) {
        if (table != null) {
            if (es != null) {
                String data = table.get(row);
                es.indexDocument(row, data);
            } else {
                LOG.error("Trying to fetchAndIndex but es is null");
            }
        } else {
            LOG.error("Trying to fetchAndIndex but table is null");
        }
    }
    
    /**
     * @param rows
     */
    public void fetchAndIndex(Collection<String> rows) {
        if (table != null) {
            if (es != null) {
                Map<String, String> dataMap = table.getAll(rows);
                es.indexDocuments(dataMap);
            } else {
                LOG.error("Trying to fetchAndIndex but es is null");
            }
        } else {
            LOG.error("Trying to fetchAndIndex but table is null");
        }
    }
    
}
