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
package com.mozilla.bagheera.rest.interceptors;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.hazelcast.core.Hazelcast;
import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;
import com.mozilla.bagheera.rest.RESTSingleton;


/**
 * Handle MDPv2 submissions
 */
public class MetricsPingPreCommit implements PreCommitHook {
    
    private static final Logger LOG = Logger.getLogger(MetricsPingPreCommit.class);
    
    private ObjectMapper objectMapper;

    public MetricsPingPreCommit() {
        objectMapper = new ObjectMapper();
    }
    
    @Override
    public String preCommit(String mapName, String id, String newDocument, HttpServletRequest request) {
        // Verify that the incoming document is valid
        // Insert GeoIP Info
        // Delete the obsolete document
        String result = null;

        try {
            ObjectNode aggregate = objectMapper.readValue(newDocument, ObjectNode.class);
            if (isValidDocument(aggregate)) {
                setGeoLocation(aggregate, request.getRemoteAddr());
                
                // TODO: Filter out only the specific data we want from the aggregate, possibly
                //       flag if there is too much else in there.
                result = objectMapper.writeValueAsString(aggregate);
                
                // Delete the key specified by the X-Obsolete-Document header
                String obsoleteDocId = request.getHeader("X-Obsolete-Document");
                if (obsoleteDocId != null) {
                    Map<String, String> m = Hazelcast.getMap(mapName);
                    m.remove(id);
                }
            } else {
                LOG.error("Invalid incoming document");
                // TODO: freak out
            }
        } catch (JsonParseException e) {
            LOG.error("Error parsing JSON Document", e);
        } catch (JsonMappingException e) {
            LOG.error("Error mapping JSON Document", e);
        } catch (IOException e) {
            LOG.error("Error reading JSON Document", e);
        }

        return result;
    }

    private void setGeoLocation(ObjectNode aggregate, String remoteIpAddress) {
        LookupService geoIpLookupService = RESTSingleton.getInstance().getGeoIpLookupService();
        if (geoIpLookupService != null) {
            Country country = geoIpLookupService.getCountry(remoteIpAddress);
            aggregate.put("geo_country", country == null ? "Unknown" : country.getCode());
            // TODO: Region-level?
        } else {
            LOG.warn("GeoIP Service is not available. Skipping GeoIP Lookup");
        }
    }
        
    // Check if all specified nodeNames exist and are ArrayNode children
    // of the given document.
    private boolean isArrayChild(JsonNode document, String... nodeNames) {
        for (String nodeName : nodeNames) {
            JsonNode aNode = document.path(nodeName);
            if (!aNode.isArray())
                return false;
        }
        return true;
    }

    protected boolean isValidDocument(JsonNode document) {
        if (document == null || !document.isObject()) return false;
        JsonNode env = document.get("env");
        if (env == null || !env.isObject()) return false;

        JsonNode dataPoints = document.get("dataPoints");
        if (dataPoints == null || !dataPoints.isObject()) return false;

        // TODO: iterate dataPoints to make sure each one is valid?
        //        - dataPoints.size() <= 180
        //        - each one contains optional search, sessions, simpleMeasurements
        if (!isArrayChild(document, "addons")) return false;
        
        String[] requiredKeys = {"lastPingTime", "thisPingTime"};
        for (String key : requiredKeys){
            if (!document.has(key)) {
                return false;
            }
        }
        
        return true;
    }
    
}