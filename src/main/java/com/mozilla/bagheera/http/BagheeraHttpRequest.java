/*
 * Copyright 2012 Mozilla Foundation
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
package com.mozilla.bagheera.http;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;

public class BagheeraHttpRequest extends DefaultHttpRequest {

    // API Version
    private static final Pattern VERSION_PATTERN = Pattern.compile("^[0-9]+([.][0-9]+)?$");

    // REST path indices
    public static final int ENDPOINT_PATH_IDX = 0;
    public static final int NAMESPACE_PATH_IDX = 1;
    public static final int ID_PATH_IDX = 2;

    private final String apiVersion;
    private final String endpoint;
    private final String namespace;
    private final String id;
    private final List<String> partitions;

    public BagheeraHttpRequest(HttpVersion httpVersion, HttpMethod method, String uri) {
        this(httpVersion, method, uri, new PathDecoder(uri));
    }

    public BagheeraHttpRequest(HttpRequest request) {
        this(request.getProtocolVersion(), request.getMethod(), request.getUri());
    }

    public BagheeraHttpRequest(HttpVersion httpVersion, HttpMethod method, String uri,
                               PathDecoder pathDecoder) {
        super(httpVersion, method, uri);
        int idxOffset = 0;

        String apiVersionIn = pathDecoder.getPathElement(0);
        // If API version is in the path then offset the path indices
        if (isApiVersion(apiVersionIn)) {
            idxOffset = 1;
            apiVersion = apiVersionIn;
        } else {
            apiVersion = null;
        }
        endpoint = pathDecoder.getPathElement(ENDPOINT_PATH_IDX + idxOffset);
        namespace = pathDecoder.getPathElement(NAMESPACE_PATH_IDX + idxOffset);
        String incomingId = pathDecoder.getPathElement(ID_PATH_IDX + idxOffset);
        if (incomingId == null) {
            incomingId = UUID.randomUUID().toString();
        }
        id = incomingId;

        int partitionOffset = idxOffset + 3;
        int numPartitions = pathDecoder.size() - partitionOffset;
        if (numPartitions < 0) {
            numPartitions = 0;
        }
        partitions = new ArrayList<String>(numPartitions);
        for (int i = partitionOffset; i < pathDecoder.size(); i++) {
            partitions.add(pathDecoder.getPathElement(i));
        }
    }

    public boolean isApiVersion(String possibleApiVersion) {
        return VERSION_PATTERN.matcher(possibleApiVersion).matches();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getId() {
        return id;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public List<String> getPartitions() {
        return partitions;
    }
}
