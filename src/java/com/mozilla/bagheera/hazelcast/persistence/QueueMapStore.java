package com.mozilla.bagheera.hazelcast.persistence;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.mozilla.bagheera.dao.HBaseTableDao;
import com.mozilla.bagheera.elasticsearch.ElasticSearchNode;

public class QueueMapStore implements MapStore<Long, String>, MapLoaderLifecycleSupport {
  ElasticSearchNode esn;
  private static final Logger LOG = Logger.getLogger(QueueMapStore.class);

  private HTablePool pool;
  private HBaseTableDao table;
  private static final String HBASE_XML_CONFIG = "conf/hbase-site.xml";

  /* (non-Javadoc)
   * @see com.hazelcast.core.MapLoaderLifecycleSupport#init(com.hazelcast.core.HazelcastInstance, java.util.Properties, java.lang.String)
   */
  public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(HBASE_XML_CONFIG));

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
    esn = new ElasticSearchNode();
    LOG.info("ElasticSearch instance started");
  }



  @Override
  public void delete(Long arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteAll(Collection<Long> arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void store(Long ooid, String restValue) {
    // TODO Auto-generated method stub

    System.out.println("received something in queue for store: " + ooid + "\tValue: " + restValue);

  }

  @Override
  public void storeAll(Map<Long, String> pairs) {
    // TODO Auto-generated method stub
    LOG.debug("QMS: received something in queue for storeAll:" + pairs.size());
    Map<String, String> ooidJsonPair = new HashMap<String, String>();

    for (Map.Entry<Long, String> pair : pairs.entrySet()) {
      LOG.debug("HazelCast key: " + pair.getKey() + " value: " + pair.getValue());
      if (StringUtils.isNotBlank(pair.getValue())) {
        //lets fetch the item from hbase
        String json = table.getJson(pair.getValue());
        if (StringUtils.isNotBlank(json)) {
          LOG.debug("ooid: " + pair.getValue() + "JSON: " + json);
          ooidJsonPair.put(pair.getValue(), json);
        } else {
          LOG.error("received blank json for ooid:" + pair.getValue());
        }
      }
    }
    if (esn == null) {
      esn = new ElasticSearchNode();
    }
    LOG.debug("Trying to index some docs inside ElasticSearch");
    if (ooidJsonPair.size() > 0) {
      if (esn.indexBulkDocument(ooidJsonPair)) {
        LOG.info("success indexing jsons inside ES, total count: " + pairs.size());
      }
    } else {
      LOG.info("nothing to index");
    }
    LOG.info("exiting storeAll");


  }

  @Override
  public String load(Long arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<Long, String> loadAll(Collection<Long> arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  public Set<Long> loadAllKeys() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void destroy() {
    // TODO Auto-generated method stub

  }


}
