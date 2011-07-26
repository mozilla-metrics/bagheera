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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.action.index.IndexRequestBuilder;

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
        } catch (ElasticSearchException e) {
            success = false;
            LOG.error("ElasticSearchException while indexing document", e);
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
                LOG.error("Received bad key or value for key: " + entry.getKey());
            }
        }

        return check(brb.execute().actionGet());
    }

    /**
     * @param ids The documents to delete.
     * @return <tt>true</tt> if no error occured, else <tt>false</tt>
     */
    public boolean delete(Iterable<String> ids) {
        BulkRequestBuilder brb = client.prepareBulk();
        for (String key : ids) {
            if (StringUtils.isNotBlank(key)) {
                brb.add(Requests.deleteRequest(indexName).type(typeName)
                        .id(key));
            } else {
                LOG.error("Trying to delete bad key: " + key);
            }
        }

        return check(brb.execute().actionGet());
    }

    public boolean delete(String key) {
        List<String> keys = new ArrayList<String>();
        keys.add(key);
        return delete(keys);
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
