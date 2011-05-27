package com.mozilla.bagheera.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchNode {

  static Logger LOG = Logger.getLogger(ElasticSearchNode.class);
  public static final String ES_INDEX_NAME = "socorro";
  //public static final String ES_INDEX_NAME = "data_current";
  public static final String ES_TYPE_NAME = "crash_reports";
  public static final String ES_FIELD_DATE_NAME = "index_submit_time";


  Client client;
  Node node;
  public  ElasticSearchNode () {
    // Set up a simple configuration that logs on the console.
    //node = nodeBuilder().loadConfigSettings(true).node().start();
    
    node = nodeBuilder().loadConfigSettings(false).settings(ImmutableSettings.settingsBuilder().loadFromSource("conf/adasd")).node().start();
    
    client = node.client();
    LOG.info("ES started");
  }

  public boolean indexBulkDocument(Map<String, String> bulkJsons) {
    boolean success = true;

    BulkRequestBuilder brb = client.prepareBulk();
    for (Map.Entry<String, String> ooidJson : bulkJsons.entrySet()) {
      LOG.debug("indexing; ooid:\t" + ooidJson.getKey());
      brb.add(Requests.indexRequest(ES_INDEX_NAME).type(ES_TYPE_NAME).id(ooidJson.getKey()).source(ooidJson.getValue()));
    }
    BulkResponse br = brb.execute().actionGet();

    if (br.hasFailures()) {
      success = false;
      for (BulkItemResponse b : br) {
        LOG.error("Error inserting ooid: " + b.getId());
        LOG.error("Error inserting ooid: " + b.getFailureMessage());
        LOG.error("Error inserting ooid: " + b.getFailure().getMessage());
        
        
      }
    }

    return success;
  }

  public boolean indexDocument(String indexString, String documentId) {
    boolean success = true;
    try {
      IndexResponse response = client.prepareIndex(ES_INDEX_NAME, ES_TYPE_NAME, documentId)
      .setSource(indexString)
      .execute()
      .actionGet();
      if (!StringUtils.equals(documentId, response.getId())) {
        LOG.error("error indexing documentId: " + documentId);
        success = false;

      } else {
        LOG.debug("successfully indexed documentId: " + documentId);
      }

    } catch (ElasticSearchException e) {
      success = false;
      e.printStackTrace();
      LOG.error("ElasticSearchException while indexing document: " + e.getMessage());
    }
    return success;
  }
}
