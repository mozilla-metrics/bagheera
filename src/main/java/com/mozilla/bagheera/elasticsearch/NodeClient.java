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
import org.elasticsearch.node.Node;

public class NodeClient {

    private static final Logger LOG = Logger.getLogger(NodeClient.class);

    private Client client;
    private Node node;
    private final String indexName;
    private final String typeName;

    public NodeClient(String indexName, String typeName) {
        this.indexName = indexName;
        this.typeName = typeName;

        node = nodeBuilder().loadConfigSettings(true).node().start();
        client = node.client();
    }

    public void close() {
        if (client != null) {
            client.close();
        }

        if (node != null) {
            node.close();
        }
    }

    public boolean indexBulkDocument(Map<String, String> dataMap) {
        boolean success = true;

        BulkRequestBuilder brb = client.prepareBulk();
        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            brb.add(Requests.indexRequest(indexName).type(typeName).id(entry.getKey()).source(entry.getValue()));
        }
        BulkResponse br = brb.execute().actionGet();
        if (br.hasFailures()) {
            success = false;
            for (BulkItemResponse b : br) {
                LOG.error("Error inserting id: " + b.getId());
                LOG.error("Failure message: " + b.getFailureMessage());
            }
        }

        return success;
    }

    public boolean indexDocument(String indexString, String documentId) {
        boolean success = true;
        try {
            IndexResponse response = client.prepareIndex(indexName, typeName, documentId).setSource(indexString)
                    .execute().actionGet();
            if (!StringUtils.equals(documentId, response.getId())) {
                LOG.error("error indexing documentId: " + documentId);
                success = false;
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("successfully indexed documentId: " + documentId);
                }
            }

        } catch (ElasticSearchException e) {
            success = false;
            LOG.error("ElasticSearchException while indexing document: " + e.getMessage(), e);
        }

        return success;
    }
}
