package com.mozilla.bagheera.hazelcast.persistence;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.HBaseTableDao;
import com.mozilla.bagheera.elasticsearch.NodeClient;

public class ElasticSearchIndexQueueStore implements MapStore<Long, String>, MapLoaderLifecycleSupport {

	private static final Logger LOG = Logger.getLogger(ElasticSearchIndexQueueStore.class);

	private HTablePool pool;
	private HBaseTableDao table;
	private NodeClient nodeClient;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.
	 * HazelcastInstance, java.util.Properties, java.lang.String)
	 */
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
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

		String indexName = properties.getProperty("hazelcast.elasticsearch.index", "default");
		String typeName = properties.getProperty("hazelcast.elasticsearch.type.name", "data");
		nodeClient = new NodeClient(indexName, typeName);
		LOG.info("ElasticSearch instance started");
	}

	@Override
	public String load(Long arg0) {
		return null;
	}

	@Override
	public Map<Long, String> loadAll(Collection<Long> arg0) {
		return null;
	}

	@Override
	public Set<Long> loadAllKeys() {
		return null;
	}

	@Override
	public void destroy() {
	}

	@Override
	public void delete(Long arg0) {
	}

	@Override
	public void deleteAll(Collection<Long> arg0) {	
	}

	@Override
	public void store(Long ooid, String restValue) {
		LOG.info("received something in queue for store: " + ooid + "\tValue: " + restValue);
    Map<String, String> idDataPair = new HashMap<String, String>();
    if (StringUtils.isNotBlank(restValue)) {
      String json = table.get(restValue);
      if (StringUtils.isNotBlank(json)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("id: " + restValue + "json: " + json);
        }        
        idDataPair.put(restValue, json);
      } else {
        LOG.error("No data for id:" + restValue);
      }
    }
    LOG.debug("Trying to index single document inside ElasticSearch");
    if (idDataPair.size() > 0) {
      if (nodeClient.indexBulkDocument(idDataPair)) {
        LOG.debug("success indexing jsons inside ES, total count: " + idDataPair.size());
      }
    } else {
      LOG.debug("nothing to index");
    }
    

	}

	@Override
	public void storeAll(Map<Long, String> pairs) {
		LOG.debug("QMS: received something in queue for storeAll:" + pairs.size());
		Map<String, String> idDataPair = new HashMap<String, String>();

		for (Map.Entry<Long, String> pair : pairs.entrySet()) {
			LOG.debug("HazelCast key: " + pair.getKey() + " value: " + pair.getValue());
			if (StringUtils.isNotBlank(pair.getValue())) {
				// lets fetch the item from hbase
				String json = table.get(pair.getValue());
				if (StringUtils.isNotBlank(json)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("id: " + pair.getValue() + "json: " + json);
					}
					idDataPair.put(pair.getValue(), json);
				} else {
					LOG.error("No data for id:" + pair.getValue());
				}
			}
		}

		LOG.debug("Trying to index some docs inside ElasticSearch");
		if (idDataPair.size() > 0) {
			if (nodeClient.indexBulkDocument(idDataPair)) {
				LOG.debug("success indexing jsons inside ES, total count: " + pairs.size());
			}
		} else {
			LOG.debug("nothing to index");
		}
	}

}
