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
package com.mozilla.bagheera.nio;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.IMap;
import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;

public class MetricsProcessor {

    private static final Logger LOG = Logger.getLogger(MetricsProcessor.class);
    
    private ObjectMapper objectMapper;
    private LookupService geoIpLookupService;
    
    public MetricsProcessor(String maxmindPath) throws IOException {
        objectMapper = new ObjectMapper();
        
        // Initialize GeoIP lookup service
        if (maxmindPath != null) {
            geoIpLookupService = new LookupService(maxmindPath, LookupService.GEOIP_MEMORY_CACHE);
        }
    }
    
    private void setGeoLocation(ObjectNode aggregate, String remoteIpAddress) {
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
    
    private boolean isValidDocument(JsonNode document) {
        if (document == null || !document.isObject()) return false;

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
    
    public HttpResponseStatus process(IMap<String,String> hcMap, String id, String newDocument, String remoteAddr, String obsoleteDocId) {
        // Verify that the incoming document is valid
        // Insert GeoIP Info
        // Delete the obsolete document
        HttpResponseStatus status = NOT_ACCEPTABLE;
        try {
            ObjectNode aggregate = objectMapper.readValue(newDocument, ObjectNode.class);
            if (isValidDocument(aggregate)) {
                setGeoLocation(aggregate, remoteAddr);
                
                // TODO: Filter out only the specific data we want from the aggregate, possibly
                //       flag if there is too much else in there.
                hcMap.put(id, objectMapper.writeValueAsString(aggregate));
                
                // Delete the key specified by the X-Obsolete-Document header
                if (obsoleteDocId != null) {
                    hcMap.remove(obsoleteDocId);
                }
                status = HttpResponseStatus.CREATED;
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
        
        return status;
    }
}
