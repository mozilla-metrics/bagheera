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
package com.mozilla.bagheera.dao;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.client.action.get.GetRequestBuilder;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;

public class ElasticSearchDao {

    private static final Logger LOG = Logger.getLogger(ElasticSearchDao.class);

    private final Client client;
    private final String indexName;
    private final String typeName;

    public ElasticSearchDao(Client client, String indexName, String typeName) {
        this.client = client;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    /**
     * @param id
     * @param source
     * @return
     */
    public boolean indexDocument(String id, String source) {
        return indexDocument(id,source,null);
    }

    /**
     * @param id
     * @param source
     * @param percolate
     * @return
     */
    public boolean indexDocument(String id, String source, String percolate) {
        if (StringUtils.isBlank(id) || StringUtils.isBlank(source)) {
            return false;
        }

        boolean success = false;
        try {
            IndexRequestBuilder requestBuilder = client.prepareIndex(indexName, typeName, id).setSource(source);
            if (percolate != null) {
                requestBuilder.setPercolate(percolate);
            }
            IndexResponse response = client.index(requestBuilder.request()).actionGet();
            if (StringUtils.equals(id, response.getId())) {
                success = true;
            }
        } catch (IndexMissingException e) {
            LOG.info("Creating index '" + indexName + "' to insert '" + id + "'", e);

        } catch (ElasticSearchException e) {
            success = false;
            LOG.error("ElasticSearchException while indexing document '" + id + "' (index '" + indexName + "'", e);
        }

        if (LOG.isDebugEnabled()) {
            if (success) {
                LOG.debug("Successfully indexed id: " + id);
            } else {
                LOG.debug("Failed to index id: " + id);
            }
        }

        return success;
    }

    /**
     * @param dataMap
     * @return
     */
    public boolean indexDocuments(Map<String, String> dataMap) {
        BulkRequestBuilder brb = client.prepareBulk();

        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                brb.add(Requests.indexRequest(indexName).type(typeName).id(entry.getKey()).source(entry.getValue()));
            } else {
                LOG.error("Received bad key or value for key: '" + entry.getKey() + "' (index '" + indexName + "'");
            }
        }

        return check(brb.execute().actionGet());
    }

    /**
     * @return A map of all documents that exist here (can be very slow and memory intensive).
     */
    public Map<String, String> fetchAll() {
        SearchRequestBuilder search = client.prepareSearch(indexName).setTypes(typeName);
        search.setQuery(QueryBuilders.matchAllQuery());
        return unwrap(client.search(search.request()).actionGet());
    }

    /**
     * @param List<String> IDs for which to get documents.
     * @return List<String> docs fresh from the index (make sure to have elasticsearch store them, and to have id indexed).
     */
    public Map<String, String> fetchAll(Iterable<String> ids) {
        SearchRequestBuilder search = client.prepareSearch(indexName).setTypes(typeName);
        BoolQueryBuilder qBuilder = QueryBuilders.boolQuery();
        for (String id : ids) {
            qBuilder.should(QueryBuilders.fieldQuery("id", id));
        }
        search.setQuery(qBuilder);
        return unwrap(client.search(search.request()).actionGet());
    }

    /** Receive a specific document from elasticsearch. Make sure source is stored. */
    public String get(String id) {
        String result = null;
        try {
            GetRequestBuilder get = client.prepareGet();
            get.setIndex(indexName).setType(typeName).setId(id);
            GetResponse response = client.get(get.request()).actionGet();
            result = response.sourceAsString();
        }
        catch (IndexMissingException e) {
            LOG.info("Tried to get '" + id + "' from missing index '" + indexName + "'");
            return null;
        }
        if (result == null) {
            LOG.error("Failed loading source of '" + id + "' (index: '" + indexName + "') from ES.");
        }
        return result;
    }

    /**
     * @param ids The documents to delete.
     * @return <tt>true</tt> if no error occured, else <tt>false</tt>
     */
    public boolean delete(Iterable<String> ids) {
        BulkRequestBuilder brb = client.prepareBulk();
        for (String id : ids) {
            if (StringUtils.isNotBlank(id)) {
                brb.add(Requests.deleteRequest(indexName).type(typeName)
                        .id(id));
            } else {
                LOG.info("Trying to delete bad key: '" + id + "' (index '" + indexName + "')");
            }
        }

        return check(brb.execute().actionGet());
    }

    /** @param id The item to delete from the index. */
    public boolean delete(String id) {
        List<String> keys = new ArrayList<String>(1);
        keys.add(id);
        return delete(keys);
    }

    private Map<String, String> unwrap(SearchResponse response) {
        Map<String, String> results = new java.util.HashMap<String, String>((int) response.hits().totalHits());
        for (SearchHit hit : response.getHits()) {
            String source = hit.sourceAsString();
            if (source == null) {
                LOG.error("Failed loading source of '" + hit.getId() + "' (index: '" + indexName + "') from ES.");
            } else {
                results.put(hit.getId(), source);
            }
        }
        return results;
    }

    private boolean check(BulkResponse response) {
        boolean success = true;

        if (response.hasFailures()) {
            success = false;
            for (BulkItemResponse b : response) {
                LOG.error("Failed to " + b.getType() + " id: " + b.getId());
                LOG.error("Failure message: " + b.getFailureMessage());
            }
        }

        return success;
    }
}
